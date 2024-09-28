import sys
import os
from importlib import import_module
from utils.redis_helper import get_redis_connection
from utils.envLoader import load_environment_variables

load_environment_variables()

def import_and_run_analyze_match():

    try:
        module = import_module("commands.arb_match")
        module.run_tfidf_analysis(prefix, category, period)
    except ImportError as e:
        print(f"Failed to import module: {e}")
    except AttributeError:
        print(f"Module does not have a run_extractor function.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    redis_db = get_redis_connection()
    if not redis_db:
        print("Failed to connect to Redis.")
        sys.exit(1)

    
    prefix = os.getenv("PREFIX")
    category =  os.getenv("CATEGORY")
    period =  os.getenv("PERIOD")
    # Ensure the consumer group exists, starting from the beginning of the stream

    # Start the asyncio event loop to listen for messages
    import_and_run_analyze_match()
