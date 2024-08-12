from utils.redis_helper import get_redis_connection
from rejson import Path
import time

def fetch_data(stream_key):
    # `stream_key` contains the key names of the JSON data
    json_keys = redis_db.xrange(stream_key)  # Modify to your actual method of getting keys from a stream
    _, entry_data = json_keys[-3]
    data_key = entry_data.get('data_key')
    print('\n',data_key,'\n')
    # Now fetch only the keys of each JSON object
    all_keys = redis_db.jsonobjkeys(data_key, "$[0:]")
    return all_keys

if __name__ == "__main__":
    redis_db = get_redis_connection()
    if redis_db:
        json_data = fetch_data('THREE_WAY_stream')
        print(json_data)
        