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
    redis_db = get_redis_connection()
    if not redis_db:
        logger.warning("Failed to connect to Redis.")
        return

    stream_name = f"{prefix}-{stream_config[category][period]}"
    least_count = stream_config[category]["least_count"]
    stream_key = f"{period}_{category}-matched_stream"
    batch_stream_count = stream_config[category][prefix]
    group_name = f"{period}_{category}-consumer_group"
    consumer_name = f"{period}_{category}-worker"
    logger.info(batch_stream_count)
    try:
        # Ensure the consumer group exists
        create_consumer_group(redis_db, stream_name, group_name)
        tokenized_stop_words = get_tokenized_stop_words(redis_db)
        message_buffer = defaultdict(lambda: defaultdict(list))
        while True:
            try:
                # `stream_key` contains the key names of the JSON data
                messages = redis_db.xreadgroup(
                    group_name, consumer_name, {stream_name: ">"}, count=1
                )
                current_time = datetime.now()
                if messages:
                    logger.info(messages)
                    # [['xtr-BTTS_upcoming_stream', [('1727462021354-0', {'data_key': 'odibets_xtr-BTTS_upcoming:1727462021-4'})]]]
                    message_id, message_data = messages[0][1][0]
                    logger.info(message_data)
                    data_key = message_data.get("data_key")
                    parts = data_key.split(":")
                    if len(parts) == 2:
                        bookmaker, suffix = (
                            parts[0].split("_")[0],
                            parts[1].split("-")[-1],
                        )
                        timestamp = (
                            int(message_id.split("-")[0]) / 1000
                        )  # Convert to seconds
                        message_time = datetime.fromtimestamp(timestamp)
                        message_buffer[suffix][bookmaker].append(
                            (message_id, data_key, message_time)
                        )
                    logger.info(f"\n{dict(message_buffer)}\n")
                    process_time_based_batches(
                        redis_db,
                        message_buffer,
                        stream_name,
                        group_name,
                        tokenized_stop_words,
                        batch_stream_count,
                        current_time,
                        period,
                        stream_key,
                        least_count,
                    )
                else:
                    time.sleep(0.1)
                    # Clear the buffer
                    # message_buffer.clear()
            except ResponseError as e:
                raise e
            except Exception as e:
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
    current_time,
    period,
    stream_key,
    least_count,
):
    time_window = timedelta(seconds=5 if period == "live" else 20)
    logger.info("processing messages")
    for suffix, suffix_data in list(message_buffer.items()):
        if len(suffix_data) >= batch_stream_count:
            batch = []
            used_bookmakers = set()
            for bookmaker, messages in suffix_data.items():
                if bookmaker not in used_bookmakers:
                    recent_messages = [
                        m for m in messages if current_time - m[2] <= time_window
                    ]
                    logger.info(f"most recent messages: {recent_messages}")
                    if recent_messages:
                        batch.append(recent_messages[0])
                        used_bookmakers.add(bookmaker)
                        if len(batch) == batch_stream_count:
                            process_batch(
                                redis_db,
                                batch,
                                stream_name,
                                group_name,
                                tokenized_stop_words,
                                stream_key,
                                least_count,
                            )
                            logger.info("processed messages")
                            # Remove processed messages
                            for b, m in zip(suffix_data.keys(), batch):
                                suffix_data[b] = [
                                    msg for msg in suffix_data[b] if msg[0] != m[0]
                                ]
                                logger.info(f"removing messages: {b} ; {m}")
                            break

        # Clean up old messages
        for bookmaker in list(suffix_data.keys()):
            logger.info("checking fr old messages")
            suffix_data[bookmaker] = [
                m for m in suffix_data[bookmaker] if current_time - m[2] <= time_window
            ]
            if not suffix_data[bookmaker]:
                logger.info(f"removing message {suffix_data[bookmaker]}")
                del suffix_data[bookmaker]

        if not suffix_data:
            logger.info(f"removing message with no match data: {message_buffer[suffix]}")
            del message_buffer[suffix]


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
