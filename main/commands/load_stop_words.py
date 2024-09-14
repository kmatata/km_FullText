import nltk
from nltk.tokenize import word_tokenize
from utils.config import stop_words
from utils.envLoader import load_environment_variables
import os
from utils.redis_helper import get_redis_connection

load_environment_variables()

# Specify the custom directory where you want to store NLTK resources
nltk_data_dir = os.path.join(
    os.path.dirname(__file__), "..", "..", "utils", "nltk_config"
)

os.makedirs(nltk_data_dir, exist_ok=True)

# Add the custom directory to the NLTK data path
nltk.data.path.append(nltk_data_dir)

if not os.path.exists(os.path.join(nltk_data_dir, "tokenizers", "punkt_tab")):
    # Ensure the directory exists
    print("Punkt tokenizer not found. Downloading...")
    nltk.download("punkt_tab", download_dir=nltk_data_dir, quiet=True)

# Get Redis connection
redis_client = get_redis_connection()

if redis_client:
    # Tokenize each stop word phrase
    tokenized_stop_words = set()
    for stop_word in stop_words:
        tokens = word_tokenize(stop_word)
        tokenized_stop_words.update(tokens)

    # Store the tokenized words in Redis as a JSON array
    redis_client.json().set("tokenized_stop_words", ".", list(tokenized_stop_words))
    print("Tokenized stop words loaded into Redis.")
else:
    print("Failed to connect to Redis. Tokenized stop words not loaded.")
