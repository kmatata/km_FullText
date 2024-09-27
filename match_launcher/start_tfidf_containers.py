from utils_launcher import get_redis_connection, check_stop_file
import sys
import os
import time
import docker
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)


def manage_docker_containers(client:docker.DockerClient, image_name, container_name, **container_options):
    try:
        container = client.containers.get(container_name)
        if container.status != "running":
            container.start()
    except docker.errors.NotFound:
        print("Container not found, creating and starting...")
        container = client.containers.run(
            image_name, detach=True, remove=True, name=container_name, network="chrome-net", **container_options
        )
    return container

def stop_containers(docker_client:docker.DockerClient, containers):
    for container_name in containers:
        try:
            container = docker_client.containers.get(container_name)
            container.stop()
            print(f"Stopped container: {container_name}")
        except docker.errors.NotFound:
            print(f"Container {container_name} not found.")
        except Exception as e:
            print(f"Error stopping container {container_name}: {e}")


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
    extractor_types = ["btts", "three_way", "double_chance"] if prefix == "lst" else ["btts"]
    active_containers = []
    print("running tfidf launchers\n")
    while True:
        try:
            if check_stop_file():
                print("Stop file detected. Stopping containers and exiting...")
                stop_containers(docker_client, active_containers)
                sys.exit(0)
            image_name = "match/tfidf"
            # Update for both live and upcoming streams for BTTS for both bookmakers
            for extractor_type in extractor_types:
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
                if container_name not in active_containers:
                    active_containers.append(container_name)
                print(f"tfidf container: {container_name}")
        except Exception as e:
            print(f"An error occurred: {e}")
        except KeyboardInterrupt:
            print("Stopping containers and exiting...")
            stop_containers(docker_client, active_containers)
            sys.exit()
        print("waiting")
        if data_source == "live":
            time.sleep(1)
        else:
            time.sleep(10)
