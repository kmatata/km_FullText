from redis.exceptions import ResponseError
from redis.commands.json.path import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from .common_utils import get_current_date, find, union, generate_match_id
from .redis_helper import trim_stream
from collections import defaultdict
import re
import difflib
from datetime import datetime


def fetch_data(redis_db, logger, json_keys):
    all_data = {}
    logger.info(f"Fetching data for {len(json_keys)} keys")
    try:
        for _, data_key in json_keys:
            logger.info(f"Processing data key: {data_key}")
            # Retrieve the paths along with the keys to keep track of their locations
            team_keys = redis_db.json().objkeys(data_key, Path("$.*.teams"))
            all_keys = [
                (str(key).lower(), index) for index, key in enumerate(team_keys)
            ]
            # print(all_keys)
            if all_keys:  # Only add if there are keys
                all_data[data_key] = all_keys
                logger.info(f"Added {len(all_keys)} keys for data key: {data_key}")
            # Acknowledge the message as processed
        return all_data
    except ResponseError as re:
        logger.critical(f"Redis response error for key {data_key}: {re}")
    except Exception as e:
        logger.critical(f"Unexpected error processing key {data_key}: {e}")
        return []


def format_similarity_results(matrix, logger, documents1, documents2):
    """Extract the top_n similarities and their indices from the sparse matrix"""
    logger.info("Formatting similarity results")
    results = []
    cx = matrix.tocoo()
    for i, j, v in zip(cx.row, cx.col, cx.data):
        results.append((documents1[i], documents2[j], v))
    # Sort results by similarity score in descending order
    results.sort(key=lambda x: x[2], reverse=True)
    logger.info(f"Formatted {len(results)} similarity results")
    return results


def update_redis_with_grouped_info(
    redis_db, group, index_mapping, stream_key, least_count, stream_name, logger
):
    logger.info(
        f"Starting Redis update for {len(group)} items in stream: {stream_name}"
    )
    logger.debug(f"Processing group indices: {group}")
    grouped_by_start_time = defaultdict(list)

    def parse_datetime(time_str):
        """Parse datetime string to datetime object"""
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse time: {time_str}, error: {e}")
            return None

    def is_time_similar(time1, time2, max_diff_minutes=0):
        """Check if two times are within acceptable range"""
        dt1 = parse_datetime(time1)
        dt2 = parse_datetime(time2)
        if not (dt1 and dt2):
            return False

        time_diff = abs((dt1 - dt2).total_seconds() / 60)
        logger.debug(
            f"Time difference between {time1} and {time2}: {time_diff} minutes"
        )
        return time_diff == max_diff_minutes

    def compare_team_names(teams1, teams2, start_time1, start_time2):
        """Compare team names considering age groups"""

        def extract_age_group(team_name):
            match = re.search(r"\bU\d+\b", team_name.upper())
            age_group = match.group(0) if match else None
            logger.debug(
                f"Extracted age group '{age_group}' from team name '{team_name}'"
            )
            return age_group

        logger.debug(f"Comparing teams - Team1: '{teams1}', Team2: '{teams2}'")

        # First check if times are similar
        if not is_time_similar(start_time1, start_time2):
            logger.info(f"Times too different: {start_time1} vs {start_time2}")
            return False


        # Split team names if they contain semicolons
        teams1_list = teams1.split(";") if isinstance(teams1, str) else [teams1]
        teams2_list = teams2.split(";") if isinstance(teams2, str) else [teams2]

        # Sort teams to ensure consistent comparison
        teams1_list = sorted(teams1_list)
        teams2_list = sorted(teams2_list)

        # If number of teams doesn't match, they're different matches
        if len(teams1_list) != len(teams2_list):
            logger.debug(
                f"Different number of teams: {len(teams1_list)} vs {len(teams2_list)}"
            )
            return False

        for t1, t2 in zip(teams1_list, teams2_list):
            age_group1 = extract_age_group(t1)
            age_group2 = extract_age_group(t2)

            # If age groups don't match, consider them different teams
            if age_group1 != age_group2:
                logger.debug(
                    f"Age groups don't match: '{age_group1}' vs '{age_group2}'"
                )
                continue

            # Remove age group suffixes for base name comparison
            base_name1 = re.sub(r"\s*U\d+\s*", "", t1).strip()
            base_name2 = re.sub(r"\s*U\d+\s*", "", t2).strip()

            similarity = difflib.SequenceMatcher(
                None, base_name1.lower(), base_name2.lower()
            ).ratio()
            logger.debug(
                f"Name similarity between '{base_name1}' and '{base_name2}': {similarity:.4f}"
            )
            if similarity > 0.71:
                logger.info(
                    f"Teams matched: '{t1}' and '{t2}' with similarity {similarity:.4f}"
                )
                return True
        logger.debug("No matching teams found")
        return False

    for index in group:
        try:
            data_key, path = index_mapping[index]
            logger.info(
                f"Processing index {index} - Data key: {data_key}, Path: {path}"
            )
            json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
            # Extract start_time from the JSON object
            start_time = json_obj.get("start_time")

            if start_time:
                current_teams = list(json_obj.get("teams", {}).keys())[0]
                logger.info(f"Extracted teams from current entry: {current_teams}")
                # Check if this entry should be grouped with existing entries
                matching_group = None
                for existing_start_time, entries in grouped_by_start_time.items():
                    logger.debug(
                        f"Checking against group with start time: {existing_start_time}"
                    )
                    for existing_entry in entries:
                        existing_teams = list(
                            existing_entry[2].get("teams", {}).keys()
                        )[0]
                        logger.debug(f"Comparing with existing teams: {existing_teams}")
                        if compare_team_names(
                            current_teams,
                            existing_teams,
                            start_time,
                            existing_entry[2].get("start_time"),
                        ):
                            matching_group = existing_start_time
                            logger.info(
                                f"Found matching group with start time: {existing_start_time}"
                            )
                            break
                    if matching_group:
                        break

                # Add to matching group or create new group
                target_start_time = matching_group if matching_group else start_time
                grouped_by_start_time[target_start_time].append(
                    (data_key, path, json_obj)
                )
                logger.info(f"Added to group with start time: {target_start_time}")
                logger.debug(
                    f"Group now contains {len(grouped_by_start_time[target_start_time])} entries"
                )
            else:
                logger.warning(
                    f"No start_time found in JSON object for key {data_key}, path {path}"
                )
        except Exception as e:
            logger.warning(f"Error processing group item: {e}")
    logger.info(f"Processing {len(grouped_by_start_time)} groups for Redis updates")
    for start_time, entries in grouped_by_start_time.items():
        logger.info(
            f"Processing group with start time {start_time}, containing {len(entries)} entries"
        )
        if len(entries) >= least_count:
            try:
                match_id = generate_match_id()
                match_key = f"matched_teams_{stream_name}:{match_id}"
                team_names = []
                match_date = None
                match_team_objects = {}
                for data_key, path, json_obj in entries:
                    bookmaker = json_obj.get("bookmaker")
                    match_date = json_obj.get("target_date")
                    team_name = list(json_obj.get("teams", {}).keys())[0]
                    match_team_objects[bookmaker] = json_obj
                    team_names.append(team_name)
                    logger.debug(f"Added team {team_name} from bookmaker {bookmaker}")
                team_names_str = ";".join(team_names)
                logger.info(f"Final team names string: {team_names_str}")
                redis_db.json().set(
                    match_key,
                    Path.root_path(),
                    {
                        "match_team_objects": match_team_objects,
                        "teams": team_names_str,
                        "created": get_current_date(),
                        "arbitrage": 0,
                        "arb_updated": 0,
                        "bookie_updated": 0,
                        "match_date": match_date,
                        "start_time": start_time,
                        "arbitrage_opportunities": [],
                    },
                )
                stream_id = redis_db.xadd(stream_key, {"data_key": match_key})
                logger.info(
                    f"Updated match info in Redis - Key: {match_key}, Teams: {team_names_str}, Match ID: {match_id}, Stream: {stream_key}, Start Time: {start_time}"
                )
            except Exception as e:
                logger.critical(
                    f"Error updating Redis for start time {start_time}: {e}"
                )


def process_batch(
    redis_db,
    batch,
    stream_name,
    group_name,
    tokenized_stop_words,
    stream_key,
    least_count,
    logger,
):
    logger.info(f"Processing batch of {len(batch)} messages")
    try:
        json_event_stream = fetch_data(
            redis_db, logger, [(msg_id, data_key) for msg_id, data_key, _ in batch]
        )

        # Prepare the document data for TF-IDF
        documents = []
        document_paths = {}
        index_mapping = {}
        global_index = 0

        for data_key, items in json_event_stream.items():
            document_paths[data_key] = []
            for key, path in items:
                documents.append(key)
                document_paths[data_key].append(global_index)
                index_mapping[global_index] = (data_key, path)
                global_index += 1

        logger.info(f"Prepared {len(documents)} documents for TF-IDF analysis")

        vectorizer = TfidfVectorizer(stop_words=tokenized_stop_words)
        combined_matrix = vectorizer.fit_transform(documents)

        parent = list(range(global_index))
        rank = [0] * global_index
        start = 0
        tfidf_matrices = {}

        for data_key, keys in document_paths.items():
            end = start + len(keys)
            tfidf_matrices[data_key] = combined_matrix[start:end]
            start = end
        logger.info("Checking for matches")
        data_keys = list(tfidf_matrices.keys())
        for i in range(len(data_keys)):
            for j in range(i + 1, len(data_keys)):
                matrix1 = tfidf_matrices[data_keys[i]]
                matrix2 = tfidf_matrices[data_keys[j]]
                C = sp_matmul_topn(
                    matrix1, matrix2.transpose(), top_n=2000, threshold=0.56
                )
                formatted_results = format_similarity_results(
                    C,
                    logger,
                    document_paths[data_keys[i]],
                    document_paths[data_keys[j]],
                )

                for doc1, doc2, score in formatted_results:
                    doc1_index = (data_keys[i], doc1)
                    doc2_index = (data_keys[j], doc2)
                    logger.info(
                        f"{data_keys[i]},{doc1} <-> {data_keys[j]},{doc2} with similarity {score:.4f}"
                    )
                    # perform union operations to link their indices, grouping related documents.
                    union(parent, rank, doc1_index[1], doc2_index[1])
                    # After all unions, find all groups

        groups = defaultdict(list)
        for index in range(global_index):
            root = find(parent, index)
            groups[root].append(index)

        logger.info(f"Formed {len(groups)} groups after similarity analysis")

        for group in groups.values():
            update_redis_with_grouped_info(
                redis_db,
                group,
                index_mapping,
                stream_key,
                least_count,
                stream_name,
                logger,
            )

        # After analysis, update Redis and acknowledge messages
        for msg_id, _, _ in batch:
            redis_db.xack(stream_name, group_name, msg_id)
            trim_stream(redis_db, stream_name, 100)

        logger.info(f"Successfully processed batch of {len(batch)} messages")
    except Exception as e:
        raise e
