import redis
import os
from .envLoader import load_environment_variables

load_environment_variables()


def get_redis_connection(db=os.getenv("REDIS_DB")):
    host = os.getenv("REDIS_HOST")
    port = int(os.getenv("REDIS_PORT"))
    db = int(db)
    password = os.getenv("REDIS_PASS", None)
    try:
        r_db = redis.Redis(
            host=host, port=port, db=db, password=password, decode_responses=True
        )
        r_db.ping()  # Check the connection
        print("Connected to Redis")
        return r_db
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return None


def get_tokenized_stop_words(redis_client:redis.Redis):
    try:
        # Retrieve the tokenized stop words from Redis
        tokenized_stop_words = redis_client.json().get("tokenized_stop_words", ".")

        if tokenized_stop_words is None:
            raise ValueError("No tokenized stop words found in Redis.")

        return list(tokenized_stop_words)

    except Exception as e:
        raise Exception(f"Failed to retrieve tokenized stop words from Redis: {e}")
    
def create_consumer_group(redis_conn:redis.Redis, stream_name, group_name):
    try:
        # Create the stream if it doesn't exist by adding an empty message and trimming it immediately.
        redis_conn.xgroup_create(stream_name, group_name, id='0-0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise

def trim_stream(r_db: redis.Redis, stream_key, max_len=1000):
    """
    Trim the stream to keep only the latest 1000 entries.
    """
    try:
        current_length = r_db.xlen(stream_key)
        if current_length > max_len:
            # Get the oldest entry (first entry in the stream)
            oldest_entry = r_db.xrange(stream_key, count=1)
            
            # Perform the trim operation
            num_trimmed = r_db.xtrim(stream_key, maxlen=max_len, approximate=False)
            
            if oldest_entry:
                oldest_id, oldest_data = oldest_entry[0]
                print(f"Trimmed {num_trimmed} entries from stream {stream_key}.")
                print(f"Oldest entry removed: ID={oldest_id}, Data={oldest_data}")
            else:
                print(f"Trimmed {num_trimmed} entries from stream {stream_key}, but couldn't retrieve the oldest entry.")
        else:
            print(f"No trimming needed for stream {stream_key}. Current length: {current_length}")
    except Exception as e:
        print(f"Error trimming stream {stream_key}: {e}")