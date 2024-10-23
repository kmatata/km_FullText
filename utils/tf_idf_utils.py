from redis.exceptions import ResponseError
from redis.commands.json.path import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from .common_utils import get_current_date, find, union, generate_match_id
from .redis_helper import trim_stream
from collections import defaultdict


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
    logger.info(f"Updating Redis with grouped info for {len(group)} items")
    logger.debug(group)
    grouped_by_start_time = defaultdict(list)
    for index in group:
        try:
            data_key, path = index_mapping[index]
            logger.info(f"data_key: {data_key} \npath: {path}")
            json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
            # Extract start_time from the JSON object
            start_time = json_obj.get("start_time")

            if start_time:
                grouped_by_start_time[start_time].append((data_key, path, json_obj))
            else:
                logger.warning(
                    f"No start_time found in JSON object for key {data_key}, path {path}"
                )
        except Exception as e:
            logger.critical(f"Error processing group item: {e}")
    for start_time, entries in grouped_by_start_time.items():
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
                team_names_str = ";".join(team_names)
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
                logger.critical(f"Error updating Redis for start time {start_time}: {e}")


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
                    matrix1, matrix2.transpose(), top_n=2000, threshold=0.5
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
