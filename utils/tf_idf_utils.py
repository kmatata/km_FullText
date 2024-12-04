from redis.exceptions import ResponseError
from redis.commands.json.path import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from .common_utils import find, union
from .redis_helper import trim_stream
from collections import defaultdict
from .grouping_utils import update_redis_with_grouped_info


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




def process_batch(
    redis_db,
    batch,
    stream_name,
    group_name,
    tokenized_stop_words,
    stream_key,
    least_count,
    period,
    market,
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
                    matrix1, matrix2.transpose(), top_n=2000, threshold=0.6
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
                period,
                market,
                logger,
            )

        # After analysis, update Redis and acknowledge messages
        for msg_id, _, _ in batch:
            redis_db.xack(stream_name, group_name, msg_id)
            trim_stream(redis_db, stream_name, 100)

        logger.info(f"Successfully processed batch of {len(batch)} messages")
    except Exception as e:
        raise e
