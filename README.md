# full text matcher*

## example log of matching sequence;

---

2024-12-12 21:10:03,483 - process_batch - INFO - 
Starting Redis update for 4 items in stream: lst-THREE_WAY_upcoming_stream

2024-12-12 21:10:03,483 - process_batch - DEBUG - 
Processing group indices: [332, 423, 920, 1259]

2024-12-12 21:10:03,483 - process_batch - INFO - 
Processing index 332 - Data key: sportPesa_lst-THREE_WAY_upcoming:1734037755-1, Path: 332

2024-12-12 21:10:03,484 - process_batch - INFO - 
Extracted teams from current entry: Brighton W;Tottenham W

2024-12-12 21:10:03,484 - process_batch - INFO - 
Creating new subgroup: a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,484 - process_batch - INFO - 
Added to group with start time: 2024-12-14 20:30:00, nested under target group -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,484 - process_batch - DEBUG - 
Group now contains 1 entries

2024-12-12 21:10:03,485 - process_batch - INFO - 
Processing index 423 - Data key: shabiki_lst-THREE_WAY_upcoming:1734037769-1, Path: 77

2024-12-12 21:10:03,485 - process_batch - INFO - 
Extracted teams from current entry: Brighton and Hove Albion WFC (Wom);Tottenham Hotspur LFC (Wom)

2024-12-12 21:10:03,485 - process_batch - DEBUG - 
Checking against group with id: a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,486 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,486 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC (Wom)

2024-12-12 21:10:03,486 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion', 'wom'}

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc (wom)

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Cleaned name: brighton and hove ()

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Team: Brighton W

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Original normalized: brighton w

2024-12-12 21:10:03,487 - process_batch - DEBUG - 
Cleaned name: brighton w

2024-12-12 21:10:03,488 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,488 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove ()', 'identifiers': {'wfc', 'albion', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,488 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,488 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,489 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,489 - process_batch - DEBUG - 
Full name similarity: 0.6000

2024-12-12 21:10:03,489 - process_batch - DEBUG - 
High name similarity (0.6000) - short-circuit match

2024-12-12 21:10:03,489 - process_batch - DEBUG - 
Team: Tottenham Hotspur LFC (Wom)

2024-12-12 21:10:03,489 - process_batch - DEBUG - 
Found identifiers: {'hotspur', 'lfc', 'wom'}

2024-12-12 21:10:03,490 - process_batch - DEBUG - 
Original normalized: tottenham hotspur lfc (wom)

2024-12-12 21:10:03,490 - process_batch - DEBUG - 
Cleaned name: tottenham ()

2024-12-12 21:10:03,490 - process_batch - DEBUG - 
Team: Tottenham W

2024-12-12 21:10:03,491 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,491 - process_batch - DEBUG - 
Original normalized: tottenham w

2024-12-12 21:10:03,491 - process_batch - DEBUG - 
Cleaned name: tottenham w

2024-12-12 21:10:03,491 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,491 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham ()', 'identifiers': {'hotspur', 'lfc', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,492 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,492 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,492 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,493 - process_batch - DEBUG - 
Full name similarity: 0.8696

2024-12-12 21:10:03,493 - process_batch - DEBUG - 
High name similarity (0.8696) - short-circuit match

2024-12-12 21:10:03,493 - process_batch - INFO - 
Found matching subgroup: id -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,494 - process_batch - INFO - 
Added to group with start time: 2024-12-14 20:30:00, nested under target group -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,494 - process_batch - DEBUG - 
Group now contains 2 entries

2024-12-12 21:10:03,494 - process_batch - INFO - 
Processing index 920 - Data key: odibets_lst-THREE_WAY_upcoming:1734037777-1, Path: 338

2024-12-12 21:10:03,496 - process_batch - INFO - 
Extracted teams from current entry: Brighton and Hove Albion WFC;Tottenham Hotspur FC

2024-12-12 21:10:03,496 - process_batch - DEBUG - 
Checking against group with id: a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,496 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Team: Brighton W

2024-12-12 21:10:03,497 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,498 - process_batch - DEBUG - 
Original normalized: brighton w

2024-12-12 21:10:03,498 - process_batch - DEBUG - 
Cleaned name: brighton w

2024-12-12 21:10:03,498 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,498 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,499 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,499 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,499 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,499 - process_batch - DEBUG - 
Full name similarity: 0.6667

2024-12-12 21:10:03,499 - process_batch - DEBUG - 
High name similarity (0.6667) - short-circuit match

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Team: Tottenham Hotspur FC

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Found identifiers: {'hotspur'}

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Original normalized: tottenham hotspur fc

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Cleaned name: tottenham fc

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Team: Tottenham W

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,500 - process_batch - DEBUG - 
Original normalized: tottenham w

2024-12-12 21:10:03,501 - process_batch - DEBUG - 
Cleaned name: tottenham w

2024-12-12 21:10:03,501 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,501 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham fc', 'identifiers': {'hotspur'}, 'has_identifiers': True}

2024-12-12 21:10:03,502 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,502 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,502 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,502 - process_batch - DEBUG - 
Full name similarity: 0.8696

2024-12-12 21:10:03,503 - process_batch - DEBUG - 
High name similarity (0.8696) - short-circuit match

2024-12-12 21:10:03,503 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,503 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,504 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,504 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,504 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,504 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC (Wom)

2024-12-12 21:10:03,505 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion', 'wom'}

2024-12-12 21:10:03,505 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc (wom)

2024-12-12 21:10:03,505 - process_batch - DEBUG - 
Cleaned name: brighton and hove ()

2024-12-12 21:10:03,505 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,505 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,506 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton and hove ()', 'identifiers': {'wfc', 'albion', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,506 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,506 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Full name similarity: 0.9189

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
High name similarity (0.9189) - short-circuit match

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Team: Tottenham Hotspur FC

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Found identifiers: {'hotspur'}

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Original normalized: tottenham hotspur fc

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Cleaned name: tottenham fc

2024-12-12 21:10:03,507 - process_batch - DEBUG - 
Team: Tottenham Hotspur LFC (Wom)

2024-12-12 21:10:03,508 - process_batch - DEBUG - 
Found identifiers: {'hotspur', 'lfc', 'wom'}

2024-12-12 21:10:03,508 - process_batch - DEBUG - 
Original normalized: tottenham hotspur lfc (wom)

2024-12-12 21:10:03,508 - process_batch - DEBUG - 
Cleaned name: tottenham ()

2024-12-12 21:10:03,508 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,508 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham fc', 'identifiers': {'hotspur'}, 'has_identifiers': True}

2024-12-12 21:10:03,508 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham ()', 'identifiers': {'hotspur', 'lfc', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,509 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,509 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,509 - process_batch - DEBUG - 
Full name similarity: 0.8333

2024-12-12 21:10:03,509 - process_batch - DEBUG - 
High name similarity (0.8333) - short-circuit match

2024-12-12 21:10:03,509 - process_batch - INFO - 
Found matching subgroup: id -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,509 - process_batch - INFO - 
Added to group with start time: 2024-12-14 20:30:00, nested under target group -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,509 - process_batch - DEBUG - 
Group now contains 3 entries

2024-12-12 21:10:03,510 - process_batch - INFO - 
Processing index 1259 - Data key: betika_lst-THREE_WAY_upcoming:1734037781-1, Path: 322

2024-12-12 21:10:03,510 - process_batch - INFO - 
Extracted teams from current entry: Brighton and Hove Albion WFC;Tottenham Women

2024-12-12 21:10:03,510 - process_batch - DEBUG - 
Checking against group with id: a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Team: Brighton W

2024-12-12 21:10:03,511 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
Original normalized: brighton w

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
Cleaned name: brighton w

2024-12-12 21:10:03,512 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,512 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,513 - process_batch - DEBUG - 
Full name similarity: 0.6667

2024-12-12 21:10:03,513 - process_batch - DEBUG - 
High name similarity (0.6667) - short-circuit match

2024-12-12 21:10:03,513 - process_batch - DEBUG - 
Team: Tottenham Women

2024-12-12 21:10:03,513 - process_batch - DEBUG - 
Found identifiers: {'women'}

2024-12-12 21:10:03,513 - process_batch - DEBUG - 
Original normalized: tottenham women

2024-12-12 21:10:03,514 - process_batch - DEBUG - 
Cleaned name: tottenham

2024-12-12 21:10:03,514 - process_batch - DEBUG - 
Team: Tottenham W

2024-12-12 21:10:03,514 - process_batch - DEBUG - 
Found identifiers: set()

2024-12-12 21:10:03,514 - process_batch - DEBUG - 
Original normalized: tottenham w

2024-12-12 21:10:03,514 - process_batch - DEBUG - 
Cleaned name: tottenham w

2024-12-12 21:10:03,515 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,515 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham', 'identifiers': {'women'}, 'has_identifiers': True}

2024-12-12 21:10:03,515 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham w', 'identifiers': set(), 'has_identifiers': False}

2024-12-12 21:10:03,515 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,515 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,516 - process_batch - DEBUG - 
Full name similarity: 0.9000

2024-12-12 21:10:03,516 - process_batch - DEBUG - 
High name similarity (0.9000) - short-circuit match

2024-12-12 21:10:03,516 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,516 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,516 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,517 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,517 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,517 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC (Wom)

2024-12-12 21:10:03,517 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion', 'wom'}

2024-12-12 21:10:03,517 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc (wom)

2024-12-12 21:10:03,518 - process_batch - DEBUG - 
Cleaned name: brighton and hove ()

2024-12-12 21:10:03,518 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,518 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,518 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton and hove ()', 'identifiers': {'wfc', 'albion', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,518 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Full name similarity: 0.9189

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
High name similarity (0.9189) - short-circuit match

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Team: Tottenham Women

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Found identifiers: {'women'}

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Original normalized: tottenham women

2024-12-12 21:10:03,519 - process_batch - DEBUG - 
Cleaned name: tottenham

2024-12-12 21:10:03,520 - process_batch - DEBUG - 
Team: Tottenham Hotspur LFC (Wom)

2024-12-12 21:10:03,520 - process_batch - DEBUG - 
Found identifiers: {'hotspur', 'lfc', 'wom'}

2024-12-12 21:10:03,520 - process_batch - DEBUG - 
Original normalized: tottenham hotspur lfc (wom)

2024-12-12 21:10:03,520 - process_batch - DEBUG - 
Cleaned name: tottenham ()

2024-12-12 21:10:03,520 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,520 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham', 'identifiers': {'women'}, 'has_identifiers': True}

2024-12-12 21:10:03,521 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham ()', 'identifiers': {'hotspur', 'lfc', 'wom'}, 'has_identifiers': True}

2024-12-12 21:10:03,521 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,521 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,521 - process_batch - DEBUG - 
Full name similarity: 0.8571

2024-12-12 21:10:03,521 - process_batch - DEBUG - 
High name similarity (0.8571) - short-circuit match

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Time difference between 2024-12-14 20:30:00 and 2024-12-14 20:30:00: 0.0 minutes

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Team: Brighton and Hove Albion WFC

2024-12-12 21:10:03,522 - process_batch - DEBUG - 
Found identifiers: {'wfc', 'albion'}

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
Original normalized: brighton and hove albion wfc

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
Cleaned name: brighton and hove

2024-12-12 21:10:03,523 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
Team1 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
Team2 components: {'name': 'brighton and hove', 'identifiers': {'wfc', 'albion'}, 'has_identifiers': True}

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,523 - process_batch - DEBUG - 
Last word (core) similarity: 1.0000

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Full name similarity: 1.0000

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
High name similarity (1.0000) - short-circuit match

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Team: Tottenham Women

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Found identifiers: {'women'}

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Original normalized: tottenham women

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Cleaned name: tottenham

2024-12-12 21:10:03,524 - process_batch - DEBUG - 
Team: Tottenham Hotspur FC

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
Found identifiers: {'hotspur'}

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
Original normalized: tottenham hotspur fc

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
Cleaned name: tottenham fc

2024-12-12 21:10:03,525 - process_batch - DEBUG - 

Comparing teams:

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
Team1 components: {'name': 'tottenham', 'identifiers': {'women'}, 'has_identifiers': True}

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
Team2 components: {'name': 'tottenham fc', 'identifiers': {'hotspur'}, 'has_identifiers': True}

2024-12-12 21:10:03,525 - process_batch - DEBUG - 
First word similarity: 1.0000

2024-12-12 21:10:03,526 - process_batch - DEBUG - 
Last word (core) similarity: 0.0000

2024-12-12 21:10:03,526 - process_batch - DEBUG - 
Full name similarity: 0.8571

2024-12-12 21:10:03,526 - process_batch - DEBUG - 
High name similarity (0.8571) - short-circuit match

2024-12-12 21:10:03,526 - process_batch - INFO - 
Found matching subgroup: id -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,526 - process_batch - INFO - 
Added to group with start time: 2024-12-14 20:30:00, nested under target group -> a10a0784-cdfc-4005-982c-60bfb573d2c6

2024-12-12 21:10:03,526 - process_batch - DEBUG - 
Group now contains 4 entries

2024-12-12 21:10:03,527 - process_batch - INFO - 
Processing 1 groups for Redis updates

2024-12-12 21:10:03,527 - process_batch - INFO - 
Processing group with start time 2024-12-14 20:30:00, containing 4 entries

2024-12-12 21:10:03,527 - process_batch - INFO - 
Generated canonical info for group a10a0784-cdfc-4005-982c-60bfb573d2c6: {'teams': {'home': 'Brighton and Hove Albion WFC', 'away': 'Tottenham Hotspur LFC (Wom)', 'source_count': 4}, 'competition': {'name': 'super league women', 'country': 'england amateur'}, 'confidence': 'HIGH'}

2024-12-12 21:10:03,527 - process_batch - DEBUG - 
Added team Brighton W;Tottenham W from bookmaker sport_pesa

2024-12-12 21:10:03,527 - process_batch - DEBUG - 
Added team Brighton and Hove Albion WFC (Wom);Tottenham Hotspur LFC (Wom) from bookmaker shabiki

2024-12-12 21:10:03,527 - process_batch - DEBUG - 
Added team Brighton and Hove Albion WFC;Tottenham Hotspur FC from bookmaker odibets

2024-12-12 21:10:03,527 - process_batch - DEBUG - 
Added team Brighton and Hove Albion WFC;Tottenham Women from bookmaker betika

2024-12-12 21:10:03,528 - process_batch - INFO - 
Final team names string: Brighton W;Tottenham W;Brighton and Hove Albion WFC (Wom);Tottenham Hotspur LFC (Wom);Brighton and Hove Albion WFC;Tottenham Hotspur FC;Brighton and Hove Albion WFC;Tottenham Women

2024-12-12 21:10:03,529 - process_batch - INFO - 
Updated match info in Redis - Key: matched_teams_lst-THREE_WAY_upcoming_stream:a10a0784-cdfc-4005-982c-60bfb573d2c6, Teams: Brighton W;Tottenham W;Brighton and Hove Albion WFC (Wom);Tottenham Hotspur LFC (Wom);Brighton and Hove Albion WFC;Tottenham Hotspur FC;Brighton and Hove Albion WFC;Tottenham Women, Match ID: a10a0784-cdfc-4005-982c-60bfb573d2c6, Stream: upcoming_three_way-matched_stream, Start Time: 2024-12-14 20:30:00

---
