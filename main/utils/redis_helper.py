from rejson import Client as RedisJSON
import os
from .envLoader import load_environment_variables

load_environment_variables()
print(os.getenv("DYNAMIC_PROXY"))


def get_redis_connection():
    host = os.getenv("REDIS_HOST")
    port = int(os.getenv("REDIS_PORT"))
    db = int(os.getenv("REDIS_DB"))
    password = os.getenv("REDIS_PASS", None)
    try:
        r_db = RedisJSON(
            host=host, port=port, db=db, password=password, decode_responses=True
        )
        r_db.ping()  # Check the connection
        print("Connected to Redis")
        return r_db
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return None


def save_data_to_redis(r_db, key, value):
    if r_db is not None:
        try:
            r_db.set(key, value)
            print(f"Data saved to Redis: {key} -> {value}")
        except Exception as e:
            print(f"Failed to save data to Redis: {str(e)}")
