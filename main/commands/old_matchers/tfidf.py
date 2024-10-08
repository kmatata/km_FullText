from utils.redis_helper import (
    get_redis_connection,
    get_tokenized_stop_words,
    create_consumer_group,
)
from redis.commands.json.path import Path
from redis.exceptions import ResponseError
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
import uuid
from utils.common_utils import get_current_date, find, union
from collections import defaultdict
import sys
from utils.config import stream_config
import time


def fetch_data(redis_db, group_name, consumer_name, stream_key):
    all_data = {}
    try:
        # `stream_key` contains the key names of the JSON data
        json_keys = redis_db.xreadgroup(
            group_name, consumer_name, {stream_key: ">"}, count=3, block=5000
        )
        if not json_keys:
            raise ValueError("data_key not found in entry.")
        for entry in json_keys[0][-1]:
            print(entry)
            id, entry_data = entry
            data_key = entry_data.get("data_key")
            print("\n", data_key)
            # Retrieve the paths along with the keys to keep track of their locations
            team_keys = redis_db.json().objkeys(data_key, "$.*.teams")
            all_keys = [
                (str(key).lower(), index) for index, key in enumerate(team_keys)
            ]
            # print(all_keys)
            if all_keys:  # Only add if there are keys
                all_data[data_key] = all_keys
            # Acknowledge the message as processed
            redis_db.xack(stream_key, group_name, id)
    except ResponseError as re:
        raise Exception(f"Redis response error: {re}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    return all_data


def format_similarity_results(matrix, documents1, documents2, top_n):
    # Extract the top_n similarities and their indices from the sparse matrix
    results = []
    cx = matrix.tocoo()
    for i, j, v in zip(cx.row, cx.col, cx.data):
        results.append((documents1[i], documents2[j], v))
    # Sort results by similarity score in descending order
    results.sort(key=lambda x: x[2], reverse=True)
    return results[:top_n]


def generate_unique_id():
    """Generate a universally unique identifier."""
    return str(uuid.uuid4())


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python -m tfidf.py [btts|double_chance|three_way] [live|upcoming]"
        )
        sys.exit(1)
    redis_db = get_redis_connection()
    if not redis_db:
        sys.exit(1)
    category, period = sys.argv[1], sys.argv[2]
    stream_name = stream_config[category][period]
    least_count = stream_config[category]["least_count"]
    stream_key = f"{period}_{category}-matched_stream"

    group_name = f"{period}_{category}-consumer_group"
    consumer_name = f"{period}_{category}-worker"

    try:
        # Ensure the consumer group exists
        create_consumer_group(redis_db, stream_name, group_name)
        tokenized_stop_words = get_tokenized_stop_words(redis_db)
        while True:
            json_event_stream = fetch_data(
                redis_db, group_name, consumer_name, stream_name
            )
            # Prepare the document data for TF-IDF
            documents = []
            document_paths = {}  # To store paths for updates
            index_mapping = {}
            global_index = 0
            for data_key, items in json_event_stream.items():
                document_paths[data_key] = []
                for key, path in items:
                    documents.append(key)
                    document_paths[data_key].append(
                        global_index
                    )  # groups document indices by their data key for easy access.
                    index_mapping[global_index] = (
                        data_key,
                        path,
                    )  # Maps global indices to their corresponding data keys and paths for later reference.
                    global_index += 1
            vectorizer = TfidfVectorizer(stop_words=tokenized_stop_words)
            combined_matrix = vectorizer.fit_transform(documents)
            # Initialize union-find structure
            parent = list(
                range(global_index)
            )  # array to hold the parent index of each document, initially pointing to itself.
            rank = [
                0
            ] * global_index  # Tracks the depth of trees in the union-find structure to optimize merging.
            start = 0
            tfidf_matrices = {}
            # Separate the combined TF-IDF matrix into sub-matrices for each data key based on the indices collected in document_paths.
            for data_key, keys in document_paths.items():
                end = start + len(keys)
                tfidf_matrices[data_key] = combined_matrix[start:end]
                start = end
                print(
                    f"TF-IDF matrix for {data_key} has shape {tfidf_matrices[data_key].shape}"
                )
            data_keys = list(tfidf_matrices.keys())
            # match_id_mapping = {}  # Map documents to a consistent match ID
            # match_counter = {}
            for i in range(len(data_keys)):
                for j in range(i + 1, len(data_keys)):
                    matrix1 = tfidf_matrices[data_keys[i]]
                    matrix2 = tfidf_matrices[data_keys[j]]
                    C = sp_matmul_topn(
                        matrix1, matrix2.transpose(), top_n=50, threshold=0.66
                    )
                    formatted_results = format_similarity_results(
                        C,
                        document_paths[data_keys[i]],
                        document_paths[data_keys[j]],
                        50,
                    )
                    print(
                        f"\nTop-10 cosine similarities between document set {i} and {j}:\n"
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

            # Update Redis with grouped information
            for group in groups.values():
                team_names = []
                match_id = generate_unique_id()
                match_key = f"matched_teams_{stream_name}:{match_id}"
                match_team_objects = {}
                match_team_objects["arbitrage"] = 0
                for index in group:
                    data_key, path = index_mapping[index]
                    team_name = redis_db.json().objkeys(
                        data_key, Path(f"$.[{path}].teams")
                    )[0][0]
                    bookamaker = redis_db.json().get(
                        data_key, Path(f"$.[{path}].bookmaker")
                    )[0]
                    bookmaker_created = redis_db.json().get(
                        data_key, Path(f"$.[{path}].create_date")
                    )[0]
                    team_names.append(team_name)
                    json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
                    match_team_objects[bookamaker] = json_obj
                    match_team_objects[f"{bookamaker}_created"] = bookmaker_created

                # indicating consensus on those matches being significant.
                # Instead of just collecting team names, you'd map each team name to the number of bookmakers that list that match.
                # After populating this mapping, you can then check if any team names meet The threshold of bookmaker listings before proceeding to save any data to Redis.

                if len(team_names) >= least_count:
                    team_names_str = ";".join(team_names)
                    redis_db.json().set(
                        match_key,
                        Path.root_path(),
                        {
                            "match_team_objects": match_team_objects,
                            "teams": team_names_str,
                            "created": get_current_date(),
                            "updated": "",
                        },
                    )
                    stream_id = redis_db.xadd(stream_key, {"data_key": match_key})
                    print(
                        f"--\nUpdated match info in Redis under key {match_key} \nwith teams {team_names_str} \nwith match_id {match_id} \nunder {stream_key}"
                    )

    except Exception as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print("Stopped by user.")
        sys.exit()



# old tfidf util

# def update_redis_with_grouped_info(
#     redis_db, group, index_mapping, match_key, stream_key, match_id, least_count
# ):
#     team_names = []
#     match_team_objects = {}
#     match_date = None
#     for index in group:
#         data_key, path = index_mapping[index]
#         team_name = redis_db.json().objkeys(data_key, Path(f"$.[{path}].teams"))[0][0]
#         bookmaker = redis_db.json().get(data_key, Path(f"$.[{path}].bookmaker"))[0]
#         match_date = redis_db.json().get(data_key, Path(f"$.[{path}].target_date"))[0]
#         json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
#         match_team_objects[bookmaker] = json_obj
#         team_names.append(team_name)

#     if len(team_names) >= least_count:
#         team_names_str = ";".join(team_names)
#         redis_db.json().merge(
#             match_key,
#             Path.root_path(),
#             {
#                 "match_team_objects": match_team_objects,
#                 "teams": team_names_str,
#                 "created": get_current_date(),
#                 "arbitrage": 0,
#                 "arb_updated": 0,
#                 "bookie_updated": 0,
#                 "match_date": match_date,
#             },
#         )
#         stream_id = redis_db.xadd(stream_key, {"data_key": match_key})
#         print(
#             f"--\nUpdated match info in Redis under key {match_key} \nwith teams {team_names_str} \nwith match_id {match_id} \nunder {stream_key}"
#         )
