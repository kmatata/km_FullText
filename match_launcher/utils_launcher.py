import psutil
import socket
import os
import redis

def get_hostname():
    """Returns the hostname @host."""
    return socket.gethostname()

def get_total_cores():
    """Returns the number of physical cores @host."""
    return psutil.cpu_count()

def get_total_memory():
    """Returns the total memory in MB."""
    memory_stats = psutil.virtual_memory()
    return memory_stats.total // (1024 ** 2)

def get_memory_details():
    """Returns memory details in MB: total, used, and free."""
    mem = psutil.virtual_memory()
    used_memory_mb = mem.used // (1024 ** 2)
    free_memory_mb = mem.available // (1024 ** 2)
    return used_memory_mb, free_memory_mb

def get_disk_details():
    """Returns disk details in GB: total, used, and free."""
    disk = psutil.disk_usage('/')
    total_disk_gb = disk.total // (1024 ** 3)
    used_disk_gb = disk.used // (1024 ** 3)
    free_disk_gb = disk.free // (1024 ** 3)
    return total_disk_gb, used_disk_gb, free_disk_gb

def get_load_average():
    """Returns the 1-minute load average as an integer."""
    return int(psutil.getloadavg()[0])

def get_redis_connection():
    host = os.getenv("REDIS_HOST")
    port = int(os.getenv("REDIS_PORT"))
    db = int(os.getenv("REDIS_DB"))
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