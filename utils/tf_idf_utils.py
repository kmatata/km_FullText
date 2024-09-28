from redis.exceptions import ResponseError
from redis.commands.json.path import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from .common_utils import get_current_date, find, union, generate_match_id
from .redis_helper import trim_stream
from collections import defaultdict


def fetch_data(redis_db, json_keys):
    all_data = {}
    try:
        for _, data_key in json_keys:
            print("\n", data_key)
            # Retrieve the paths along with the keys to keep track of their locations
            team_keys = redis_db.json().objkeys(data_key, Path("$.*.teams"))
            all_keys = [
                (str(key).lower(), index) for index, key in enumerate(team_keys)
            ]
            # print(all_keys)
            if all_keys:  # Only add if there are keys
                all_data[data_key] = all_keys
            # Acknowledge the message as processed
        return all_data
    except ResponseError as re:
        raise Exception(f"Redis response error: {re}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []


def format_similarity_results(matrix, documents1, documents2):
    # Extract the top_n similarities and their indices from the sparse matrix
    results = []
    cx = matrix.tocoo()
    for i, j, v in zip(cx.row, cx.col, cx.data):
        results.append((documents1[i], documents2[j], v))
    # Sort results by similarity score in descending order
    results.sort(key=lambda x: x[2], reverse=True)
    return results


def update_redis_with_grouped_info(
    redis_db, group, index_mapping, match_key, stream_key, match_id, least_count
):
    team_names = []
    match_team_objects = {}
    match_date = None
    for index in group:
        data_key, path = index_mapping[index]
        team_name = redis_db.json().objkeys(data_key, Path(f"$.[{path}].teams"))[0][0]
        bookmaker = redis_db.json().get(data_key, Path(f"$.[{path}].bookmaker"))[0]
        match_date = redis_db.json().get(data_key, Path(f"$.[{path}].target_date"))[0]
        json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
        match_team_objects[bookmaker] = json_obj
        team_names.append(team_name)

    if len(team_names) >= least_count:
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
            },
        )
        stream_id = redis_db.xadd(stream_key, {"data_key": match_key})
        print(
            f"--\nUpdated match info in Redis under key {match_key} \nwith teams {team_names_str} \nwith match_id {match_id} \nunder {stream_key}"
        )


def process_batch(
    redis_db,
    batch,
    stream_name,
    group_name,
    tokenized_stop_words,
    stream_key,
    least_count,
):
    try:
        json_event_stream = fetch_data(
            redis_db, [(msg_id, data_key) for msg_id, data_key, _ in batch]
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
        print("checking match")
        data_keys = list(tfidf_matrices.keys())
        for i in range(len(data_keys)):
            for j in range(i + 1, len(data_keys)):
                matrix1 = tfidf_matrices[data_keys[i]]
                matrix2 = tfidf_matrices[data_keys[j]]
                C = sp_matmul_topn(
                    matrix1, matrix2.transpose(), top_n=1000, threshold=0.66
                )
                formatted_results = format_similarity_results(
                    C,
                    document_paths[data_keys[i]],
                    document_paths[data_keys[j]],
                )

                for doc1, doc2, score in formatted_results:
                    # Resolve or generate unique match ID
                    # Here you would update Redis based on doc1 and doc2 paths to link them by a unique identifier
                    doc1_index = (data_keys[i], doc1)
                    doc2_index = (data_keys[j], doc2)
                    print(
                        f"{data_keys[i]},{doc1} <-> {data_keys[j]},{doc2} with similarity {score:.4f}"
                    )
                    # perform union operations to link their indices, grouping related documents.
                    union(parent, rank, doc1_index[1], doc2_index[1])
                    # After all unions, find all groups

        groups = defaultdict(list)
        for index in range(global_index):
            root = find(parent, index)
            groups[root].append(index)

        for group in groups.values():
            match_id = generate_match_id()
            match_key = f"matched_teams_{stream_name}:{match_id}"
            update_redis_with_grouped_info(
                redis_db,
                group,
                index_mapping,
                match_key,
                stream_key,
                match_id,
                least_count,
            )

        # After analysis, update Redis and acknowledge messages
        for msg_id, _, _ in batch:
            redis_db.xack(stream_name, group_name, msg_id)
            trim_stream(redis_db, stream_name, 100)

        print(f"Processed batch of {len(batch)} messages")
    except Exception as e:
        raise e
