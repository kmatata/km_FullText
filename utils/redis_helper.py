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
