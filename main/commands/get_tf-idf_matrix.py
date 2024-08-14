from utils.redis_helper import get_redis_connection, get_tokenized_stop_words
from rejson import Path
import time
import redis
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn


def fetch_data(stream_key):
    all_data = {}
    try:
        # `stream_key` contains the key names of the JSON data
        json_keys = redis_db.xrevrange(
            stream_key
        )  # Modify to your actual method of getting keys from a stream
        if not json_keys:
            raise ValueError("data_key not found in entry.")
        for entry in json_keys:
            _, entry_data = entry
            data_key = entry_data.get("data_key")
            print("\n", data_key)
            # Now fetch only the keys of each JSON object
            all_keys = redis_db.jsonobjkeys(data_key, "$.[0:]")
            # print('\n',all_keys)
            all_data[data_key] = [str(key).lower() for key in all_keys]
    except redis.exceptions.ResponseError as re:
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


if __name__ == "__main__":
    redis_db = get_redis_connection()
    if redis_db:
        try:
            tokenized_stop_words = get_tokenized_stop_words(redis_db)
            json_event_stream = fetch_data("THREE_WAY_stream")
            all_team_keys = [
                key for keys in json_event_stream.values() for key in keys
            ]  # Single flatten operation

            vectorizer = TfidfVectorizer(stop_words=tokenized_stop_words)
            combined_matrix = vectorizer.fit_transform(all_team_keys)

            start = 0
            tfidf_matrices = {}
            document_sets = []
            for data_key, keys in json_event_stream.items():
                end = start + len(keys)
                tfidf_matrices[data_key] = combined_matrix[start:end]
                document_sets.append(keys)
                start = end
                print(
                    f"TF-IDF matrix for {data_key} has shape {tfidf_matrices[data_key].shape}"
                )

            for i, (doc_set1, matrix1) in enumerate(tfidf_matrices.items()):
                for j, (doc_set2, matrix2) in enumerate(
                    list(tfidf_matrices.items())[i + 1 :], i + 1
                ):
                    C = sp_matmul_topn(
                        matrix1, matrix2.transpose(), top_n=50, threshold=0.7
                    )
                    formatted_results = format_similarity_results(
                        C, document_sets[i], document_sets[j], 50
                    )
                    print(
                        f"\nTop-10 cosine similarities between document set {i} and {j}:\n"
                    )
                    for doc1, doc2, score in formatted_results:
                        print(f"{doc1} <-> {doc2} with similarity {score:.4f}")

        except Exception as e:
            print(f"An error occurred: {e}")
