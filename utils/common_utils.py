import pytz
from datetime import datetime

def get_current_date():
    # Define the East African Timezone (EAT)
    eat_tz = pytz.timezone('Africa/Nairobi')
    
    # Get the current time and convert it to EAT
    now = datetime.now(eat_tz)
    
    # Return the numeric timestamp
    return int(now.timestamp())

def find(parent, i):
    if parent[i] == i:
        return i
    else:
        return find(parent, parent[i])

def union(parent, rank, x, y):
    xroot = find(parent, x)
    yroot = find(parent, y)
    if xroot != yroot:
        if rank[xroot] < rank[yroot]:
            parent[xroot] = yroot
        elif rank[xroot] > rank[yroot]:
            parent[yroot] = xroot
        else:
            parent[yroot] = xroot
            rank[xroot] += 1