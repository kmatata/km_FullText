from utils_launcher import check_stop_file
import sys
import os
import time
import docker
from dotenv import load_dotenv
from logger_byLauncher import Logger
import traceback

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)
logger = Logger(__name__)
logger.clear_old_logs()
logger.info(f"RUNNING FILE \n\n @---{__file__}---\n")


def manage_docker_containers(
    client: docker.DockerClient, image_name, container_name, **container_options
):
    try:
        container = client.containers.get(container_name)
        if container.status != "running":
            container.start()
    except docker.errors.NotFound:
        logger.info("Container not found, creating and starting...")
        container = client.containers.run(
            image_name,
            detach=True,
            # remove=True,
            name=container_name,
            network="chrome-net",
            **container_options,
        )
    return container


def stop_containers(docker_client: docker.DockerClient, containers):
    for container_name in containers:
        try:
            container = docker_client.containers.get(container_name)
            container.stop()
            logger.info(f"Stopped container: {container_name}")
        except docker.errors.NotFound:
            logger.warning(f"Container {container_name} not found.")
        except Exception as e:
            logger.warning(f"Error stopping container {container_name}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.warning("Usage: python match_launcher/start_tfidf_containers.py <lst|xtr> <data_source: live|upcoming>")
        sys.exit(1)

    prefix = sys.argv[1]
    data_source = sys.argv[2]
    docker_client = docker.from_env()
    extractor_types = (
        ["btts", "three_way", "double_chance"] if prefix == "lst" else ["btts"]
    )
    active_containers = []
    logger.info("running tfidf launchers\n")
    while True:
        try:
            if check_stop_file():
                logger.info("Stop file detected. Stopping containers and exiting...")
                stop_containers(docker_client, active_containers)
                sys.exit(0)
            image_name = "match/tfidf"
            # Update for both live and upcoming streams for BTTS for both bookmakers
            for extractor_type in extractor_types:
                container_name = f"{prefix}tfidf_{extractor_type}_{data_source}"
                container_options = {
                    "environment": [
                        "PREFIX=" + prefix,
                        "CATEGORY=" + extractor_type,
                        "PERIOD=" + data_source,
                    ]
                }
                container = manage_docker_containers(
                    docker_client, image_name, container_name, **container_options
                )
                if container_name not in active_containers:
                    active_containers.append(container_name)
                logger.info(f"tfidf container: {container_name}")
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_details = traceback.extract_tb(exc_traceback)

            error_message = f"An error occurred: {str(e)}\n"
            error_message += "Traceback (most recent call last):\n"
            for frame in error_details:
                filename = os.path.basename(frame.filename)
                error_message += (
                    f'  File "{filename}", line {frame.lineno}, in {frame.name}\n'
                )
                error_message += f"    {frame.line}\n"
            error_message += f"{exc_type.__name__}: {str(e)}"
            logger.warning(f"An error occurred: {error_message}")
            sys.exit(1)
        except KeyboardInterrupt:
            logger.warning("Stopping containers and exiting...")
            sys.exit()
        logger.info("waiting")
        time.sleep(60)
