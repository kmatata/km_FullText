from utils.redis_helper import (
    get_redis_connection,
    get_tokenized_stop_words,
    create_consumer_group,
)
from redis.exceptions import ResponseError
from utils.tf_idf_utils import process_batch
from collections import defaultdict
import sys
from utils.config import stream_config
import time
from datetime import datetime, timedelta
import sys
import os
import traceback
from utils.logger import Logger

logger = Logger(__name__)
logger.info(f"RUNNING FILE \n\n @---{__file__}---\n")


def run_tfidf_analysis(prefix, category, period):
    logger.info(
        f"Starting TFIDF analysis for prefix: {prefix}, category: {category}, period: {period}"
    )
    redis_db = get_redis_connection()
    if not redis_db:
        logger.error("Failed to connect to Redis.")
        return
    TIMESTAMP_DIVISOR = 40
    stream_name = f"{prefix}-{stream_config[category][period]}"
    least_count = stream_config[category]["least_count"]
    stream_key = f"{period}_{category}-matched_stream"
    batch_stream_count = stream_config[category][prefix]
    group_name = f"{period}_{category}-consumer_group"
    consumer_name = f"{period}_{category}-worker"
    logger.info(
        f"Configuration - Stream: {stream_name}, Batch Count: {batch_stream_count}, Group: {group_name}"
    )
    try:
        # Ensure the consumer group exists
        create_consumer_group(redis_db, stream_name, group_name)
        logger.info(f"Consumer group '{group_name}' created or already exists")

        tokenized_stop_words = get_tokenized_stop_words(redis_db)

        message_buffer = defaultdict(lambda: defaultdict(list))
        logger.info("Entering main processing loop")

        while True:
            try:
                # `stream_key` contains the key names of the JSON data
                messages = redis_db.xreadgroup(
                    group_name, consumer_name, {stream_name: ">"}, count=1
                )
                if messages:
                    current_timestamp = int(time.time())
                    logger.info(f"Received message: \n{messages}")
                    # [['xtr-BTTS_upcoming_stream', [('1727462021354-0', {'data_key': 'odibets_xtr-BTTS_upcoming:1727462021-4'})]]]
                    # [['xtr-BTTS_upcoming_stream', [('1727462021354-0', {'data_key': 'betika_xtr-BTTS_upcoming:1727462026-4'})]]]
                    message_id, message_data = messages[0][1][0]
                    data_key = message_data.get("data_key")
                    logger.info(
                        f"Processing message - ID: {message_id}, Data Key: {data_key}"
                    )

                    parts = data_key.split(":")
                    if len(parts) == 2:
                        bookmaker, timestamp_suffix = parts[0].split("_")[0], parts[
                            1
                        ].split("-")
                        timestamp = int(timestamp_suffix[0])
                        date_index = timestamp_suffix[1]
                        message_buffer[date_index][timestamp // TIMESTAMP_DIVISOR].append(
                            (message_id, data_key, bookmaker)
                        )
                        logger.info(
                            f"Added message to buffer - Date Index: {date_index}, Timestamp Group: {timestamp // TIMESTAMP_DIVISOR}"
                        )

                    logger.info(
                        f"Current message buffer state: \n{dict(message_buffer)}"
                    )
                    process_time_based_batches(
                        redis_db,
                        message_buffer,
                        stream_name,
                        group_name,
                        tokenized_stop_words,
                        batch_stream_count,
                        stream_key,
                        current_timestamp,
                        least_count,
                    )
                else:
                    logger.info("No new messages, sleeping for 0.1 seconds")
                    time.sleep(0.1)
                    # Clear the buffer
                    # message_buffer.clear()
            except ResponseError as e:
                logger.error(f"Redis ResponseError: {str(e)}")
                raise e
            except Exception as e:
                logger.error(f"Unexpected error in message processing loop: {str(e)}")
                raise e
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
        logger.error(f"An error occurred: {error_message}")
        return


def process_time_based_batches(
    redis_db,
    message_buffer,
    stream_name,
    group_name,
    tokenized_stop_words,
    batch_stream_count,
    stream_key,
    current_timestamp,
    least_count,
):
    logger.info("Starting to process time-based batches")
    for date_index, timestamp_data in list(message_buffer.items()):
        logger.info(f"Processing date index: {date_index}")
        for timestamp_group, messages in list(timestamp_data.items()):
            logger.info(
                f"Processing timestamp group: {timestamp_group}, Messages count: {len(messages)}"
            )
            if not messages:  # Remove empty lists immediately
                logger.info(f"Removing empty timestamp group: {timestamp_group}")
                del timestamp_data[timestamp_group]
                continue
            unique_bookmakers = set(msg[2] for msg in messages)
            logger.info(f"Unique bookmakers in current batch: {unique_bookmakers}")

            if len(unique_bookmakers) >= batch_stream_count:
                logger.info(
                    f"Sufficient unique bookmakers ({len(unique_bookmakers)}) for batch processing"
                )
                batch = []
                # we can also have a used set of date indices 0,1,2...etc..remove the confusion of time based processing

                for bookmaker in unique_bookmakers:
                    msg = next((m for m in messages if m[2] == bookmaker), None)
                    if msg:
                        batch.append(msg)
                        if len(batch) >= batch_stream_count:
                            logger.info(
                                f"Batch complete. Processing {len(batch)} messages"
                            )
                            process_batch(
                                redis_db,
                                batch,
                                stream_name,
                                group_name,
                                tokenized_stop_words,
                                stream_key,
                                least_count,
                                logger,
                            )
                            logger.info("Batch processed successfully")
                            # Remove processed messages
                            timestamp_data[timestamp_group] = [
                                m for m in messages if m[0] not in [b[0] for b in batch]
                            ]
                            logger.info(
                                f"Remaining messages in timestamp group: {len(timestamp_data[timestamp_group])}"
                            )
                            logger.info("Finished processing time-based batches for the current iteration")
                            break

            else:
                logger.info(
                    f"Insufficient unique bookmakers ({len(unique_bookmakers)}) for batch processing"
                )

            # Clean up old messages
            if messages:
                # Use the timestamp from the most recent message in the group
                latest_message_time = max(int(msg[1].split(':')[1].split('-')[0]) for msg in messages)
                group_age = current_timestamp - latest_message_time
                if group_age > 30:  # Remove groups older than 30secs
                    logger.info(f"Removing old timestamp group: {timestamp_group}, age: {group_age} seconds")
                    del timestamp_data[timestamp_group]
        # Remove empty timestamp groups
        timestamp_data = {k: v for k, v in timestamp_data.items() if v}
        if not timestamp_data:
            logger.info(f"Removing empty date index: {date_index}")
            del message_buffer[date_index]
    # Final cleanup of any remaining empty date indices
    message_buffer = {k: v for k, v in message_buffer.items() if v}

    logger.info(f"current message buffer state: {dict(message_buffer)}")
    logger.info("nothing processed, quitting processing time-based batches")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.warning(
            "Usage: python -m main.commands.arb_match xtr|lst [btts|double_chance|three_way] [live|upcoming]"
        )
        sys.exit(1)
    prefix, category, period = sys.argv[1], sys.argv[2], sys.argv[3]
    try:
        run_tfidf_analysis(prefix, category, period)
    except Exception as e:
        logger.warning(f"An error occurred: {e}")
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
        sys.exit()
