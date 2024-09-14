from utils_launcher import get_redis_connection
import sys
import os
import time
import docker
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)


def manage_docker_containers(client, image_name, container_name, **container_options):
    try:
        container = client.containers.get(container_name)
        if container.status != "running":
            container.start()
    except docker.errors.NotFound:
        print("Container not found, creating and starting...")
        container = client.containers.run(
            image_name, detach=True, name=container_name, network="chrome-net", **container_options
        )
    return container


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <lst|xtr> <data_source: live|upcoming>")
        sys.exit(1)

    prefix = sys.argv[1]
    data_source = sys.argv[2]
    docker_client = docker.from_env()
    redis_client = get_redis_connection()
    if not redis_client:
        sys.exit(1)

    while True:
        try:
            image_name = "match/tfidf"
            # Update for both live and upcoming streams for BTTS for both bookmakers
            for extractor_type in ["btts", "three_way", "double_chance"]:
                container_name = f"{prefix}tfidf_{extractor_type}_{data_source}"
                container_options = {
                    "environment": [
                        "PREFIX=" + sys.argv[1],
                        "CATEGORY=" + extractor_type,
                        "PERIOD=" + data_source,
                    ]
                }
                container = manage_docker_containers(
                    docker_client, image_name, container_name, **container_options
                )

        except Exception as e:
            print(f"An error occurred: {e}")
        except KeyboardInterrupt:
            print("Stopped by user.")
            sys.exit()
        if data_source == "live":
            time.sleep(10)
        else:
            time.sleep(60)
