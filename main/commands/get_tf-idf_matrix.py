from utils.redis_helper import get_redis_connection
from rejson import Path
import time
import redis
from sklearn.feature_extraction.text import TfidfVectorizer


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
            all_data[data_key] = all_keys
    except redis.exceptions.ResponseError as re:
        raise Exception(f"Redis response error: {re}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    return all_data


def extract_keys(json_objects):
    if not json_objects:
        return []
    keys = []
    for obj in json_objects:
        keys.extend(name.lower() for name in obj)
    return keys


def create_combined_tfidf_matrix(documents_list: list):
    vectorizer = TfidfVectorizer()
    combined_documents = [
        doc for sublist in documents_list for doc in sublist
    ]  # Flatten the list of lists
    combined_tfidf_matrix = vectorizer.fit_transform(combined_documents)
    return vectorizer, combined_tfidf_matrix


if __name__ == "__main__":
    redis_db = get_redis_connection()
    if redis_db:
        try:
            json_event_stream = fetch_data("THREE_WAY_stream")
            tfidf_matrices = {}
            all_team_keys = []
            for data_key, keys in json_event_stream.items():
                team_keys = extract_keys(keys)
                all_team_keys.append(team_keys)
                print(team_keys, "\n")
            documents1 = all_team_keys[0] if len(all_team_keys) > 0 else []
            documents2 = all_team_keys[1] if len(all_team_keys) > 1 else []

            # Create a combined TF-IDF vectorizer and matrix for all documents
            vectorizer, combined_matrix = create_combined_tfidf_matrix(
                [documents1, documents2]
            )

            # Now extract submatrices for each original set of documents
            tfidf_matrix1 = vectorizer.transform(documents1)
            print(f"TF-IDF matrix for data_key1 has shape {tfidf_matrix1.shape}")
            tfidf_matrix2 = vectorizer.transform(documents2)

            print(f"TF-IDF matrix for data_key2 has shape {tfidf_matrix2.shape}")

        except Exception as e:
            print(f"An error occurred: {e}")
