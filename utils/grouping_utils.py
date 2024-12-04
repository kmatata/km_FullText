from redis.commands.json.path import Path
from .common_utils import get_current_date, generate_match_id
from collections import defaultdict
import re
import difflib
from datetime import datetime
from .config import match_words, ABBREVIATION_MAPPINGS, IDENTIFIER_GROUPS

IDENTIFIER_TO_GROUP = {}

# Validate all identifiers are in match_words
# for identifiers in IDENTIFIER_GROUPS.values():
#     for identifier in identifiers:
#         assert identifier in match_words, f"Identifier {identifier} not in match_words"


def get_canonical_names(entries):
    """
    Derive canonical names from pre-matched entries.
    At this point, entries are already confirmed to be the same match.
    """
    # Get all variations
    team_variations = []
    comp_variations = []

    for _, _, json_obj in entries:
        teams = list(json_obj.get("teams", {}).keys())[0].split(";")
        team_variations.append(teams)
        comp_name = json_obj.get("competition_name", "")
        comp_variations.append(comp_name)

    # Get most common team names
    home_teams, away_teams = zip(*team_variations)
    canonical_home = max(set(home_teams), key=home_teams.count)
    canonical_away = max(set(away_teams), key=away_teams.count)

    # Get competition and country
    # Since we know they're related, just take most common
    comp_parts = [c.split("_") for c in comp_variations]
    countries = [p[-1].strip() for p in comp_parts]
    competition = max(
        set([p[0].strip() for p in comp_parts]),
        key=lambda x: sum(c.startswith(x) for c, _ in comp_parts),
    )
    country = max(set(countries), key=countries.count) if countries else None

    return {
        "teams": {
            "home": canonical_home,
            "away": canonical_away,
            "source_count": len(entries),
        },
        "competition": {"name": competition, "country": country},
        "confidence": "HIGH" if len(entries) >= 3 else "MEDIUM",
    }


def update_redis_with_grouped_info(
    redis_db,
    group,
    index_mapping,
    stream_key,
    least_count,
    stream_name,
    period,
    market,
    logger,
):
    """
    Compare team names with both annotated and plain team names, using intelligent
    identifier grouping for better matching.

    Args:
        redis_db: Redis database connection
        group: List of indices to process
        index_mapping: Mapping of indices to data keys and paths
        stream_key: Key for the Redis stream
        least_count: Minimum number of entries needed for a valid group
        stream_name: Name of the stream
        logger: Logger instance for tracking comparison process
    """
    # Create reverse mapping for quick lookup
    logger.debug(f"creating reverse lookup for identifiers in advance")
    for group_name, ids in IDENTIFIER_GROUPS.items():
        for identifier in ids:
            IDENTIFIER_TO_GROUP[identifier] = group_name
    logger.info(
        f"Starting Redis update for {len(group)} items in stream: {stream_name}"
    )
    logger.debug(f"Processing group indices: {group}")
    # nested defaultdict structure
    grouped_by_start_time = defaultdict(lambda: defaultdict(list))

    def parse_datetime(time_str):
        """Parse datetime string to datetime object"""
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse time: {time_str}, error: {e}")
            return None

    def is_time_similar(time1, time2, max_diff_minutes=0, period="upcoming"):
        """Check if two times are within acceptable range"""
        dt1 = parse_datetime(time1)
        dt2 = parse_datetime(time2)
        if not (dt1 and dt2):
            return False

        time_diff = abs((dt1 - dt2).total_seconds() / 60)

        # For live matches, allow ±25 minute buffer
        if period == "live":
            is_within_buffer = time_diff <= 25
            logger.debug(
                f"Live match time difference: {time_diff} minutes, within buffer: {is_within_buffer}"
            )
            return is_within_buffer

        logger.debug(
            f"Time difference between {time1} and {time2}: {time_diff} minutes"
        )
        return time_diff == max_diff_minutes

    def find_grouped_identifiers(identifiers):
        """
        Convert individual identifiers to their group identifiers for better matching
        """
        grouped = set()
        raw = set()
        expanded = set()

        for identifier in identifiers:
            # Keep original identifier
            raw.add(identifier)
            # Expand abbreviations
            if identifier.lower() in ABBREVIATION_MAPPINGS:
                expanded_form = ABBREVIATION_MAPPINGS[identifier.lower()]
                expanded.add(expanded_form)
                # Add individual words from expanded form
                expanded.update(expanded_form.split())
            # Add group name if identifier is part of a group
            if identifier in IDENTIFIER_TO_GROUP:
                group_name = IDENTIFIER_TO_GROUP[identifier]
                grouped.add(group_name)
                logger.debug(
                    f"Mapped identifier '{identifier}' to group '{group_name}'"
                )

        # Add expanded forms to raw set
        raw.update(expanded)

        logger.debug(f"Raw identifiers: {raw}")
        logger.debug(f"Expanded identifiers: {expanded}")
        logger.debug(f"Grouped identifiers: {grouped}")

        return {
            "raw": raw,
            "grouped": grouped,
            "expanded": expanded,
            "semantic_count": len(grouped),
        }

    def calculate_identifier_similarity(identifiers1, identifiers2):
        """
        Calculate similarity between identifier sets using both raw and grouped identifiers
        """
        group1 = find_grouped_identifiers(identifiers1)
        group2 = find_grouped_identifiers(identifiers2)

        # Calculate raw similarity
        common_raw = group1["raw"] & group2["raw"]
        all_raw = group1["raw"] | group2["raw"]
        raw_similarity = len(common_raw) / len(all_raw) if all_raw else 1.0

        # Calculate grouped similarity
        common_grouped = group1["grouped"] & group2["grouped"]
        all_grouped = group1["grouped"] | group2["grouped"]
        # Calculate semantic similarity based on matched groups
        if common_grouped:
            semantic_similarity = len(common_grouped) / max(
                group1["semantic_count"], group2["semantic_count"]
            )
        else:
            semantic_similarity = 0.0

        # Weight semantic matches higher
        final_similarity = (raw_similarity * 0.3) + (semantic_similarity * 0.7)

        logger.debug(f"Raw similarity: {raw_similarity}")
        logger.debug(f"Common groups: {common_grouped}")
        logger.debug(f"All groups: {all_grouped}")
        logger.debug(f"Semantic similarity: {semantic_similarity}")
        logger.debug(f"Final identifier similarity: {final_similarity}")

        return final_similarity

    def normalize_team_name(team_name):
        """Extract identifiers and base name, handling cases with no identifiers"""
        normalized = team_name.lower().strip()

        # Find matching identifiers from match_words
        identifiers = set()
        for word in match_words:
            if re.search(rf"\b{re.escape(word)}\b", normalized):
                identifiers.add(word)

        logger.debug(f"Team: {team_name}")
        logger.debug(f"Found identifiers: {identifiers}")
        logger.debug(f"Normalized: {normalized}")

        return {
            "name": normalized,
            "identifiers": identifiers,
            "has_identifiers": bool(identifiers),
        }

    def compare_teams(team1, team2):
        """Compare teams, adapting to presence/absence of identifiers"""
        comp1 = normalize_team_name(team1)
        comp2 = normalize_team_name(team2)

        logger.debug(f"\nComparing teams:")
        logger.debug(f"Team1 components: {comp1}")
        logger.debug(f"Team2 components: {comp2}")

        # Calculate base name similarity using just the last word if multiple words exist
        def get_core_name(name):
            words = name.split()
            return (
                words[-1] if words else name
            )  # Return last word or full name if single word

        name1_core = get_core_name(comp1["name"])
        name2_core = get_core_name(comp2["name"])
        core_similarity = difflib.SequenceMatcher(None, name1_core, name2_core).ratio()

        # Calculate base name similarity
        full_similarity = difflib.SequenceMatcher(
            None, comp1["name"], comp2["name"]
        ).ratio()
        logger.debug(f"Team Name similarity: {full_similarity}")

        # Use the maximum of core and full similarity
        name_similarity = max(core_similarity, full_similarity)
        logger.debug(
            f"Team Name similarity - Core: {core_similarity:.4f}, Full: {full_similarity:.4f}"
        )

        # Short-circuit on high name similarity
        if full_similarity >= 0.59:
            logger.debug(
                f"High name similarity ({full_similarity:.4f}) - short-circuit match"
            )
            return full_similarity, True

        # Short-circuit on very high core name similarity
        if core_similarity >= 0.9:  # Higher threshold for core name
            logger.debug(
                f"High core name similarity ({core_similarity:.4f}) - short-circuit match"
            )
            return core_similarity, True

        # If neither team has identifiers, use only name similarity
        if not (comp1["has_identifiers"] or comp2["has_identifiers"]):
            logger.debug("No identifiers found - using pure name similarity")
            should_match = name_similarity >= 0.65
            return name_similarity, should_match

        # If only one team has identifiers, use weighted approach
        if comp1["has_identifiers"] != comp2["has_identifiers"]:
            logger.debug("Asymmetric identifiers - using weighted approach")
            # Higher weight on name similarity when identifiers don't match
            if core_similarity >= 0.7:
                should_match = True
                logger.debug(
                    f"High core similarity ({core_similarity:.4f}) with asymmetric identifiers - accepting match"
                )
            else:
                should_match = name_similarity >= 0.7
            return name_similarity, should_match

        # Calculate identifier similarity using grouped matching
        identifier_similarity = calculate_identifier_similarity(
            comp1["identifiers"],
            comp2["identifiers"],
        )

        # Calculate final similarity with adjusted weights
        # Give more weight to identifier matching for teams with identifiers
        total_similarity = (name_similarity * 0.6) + (identifier_similarity * 0.4)
        logger.debug(f"Total similarity: {total_similarity}")

        should_match = total_similarity >= 0.5
        return total_similarity, should_match

    def compare_team_names(teams1, teams2):
        """
        Compare team names with proper handling of optional identifiers
        """

        teams1_list = teams1.split(";") if isinstance(teams1, str) else [teams1]
        teams2_list = teams2.split(";") if isinstance(teams2, str) else [teams2]

        if len(teams1_list) != len(teams2_list):
            return 0.0, False  # Return tuple with similarity and match status

        # Compare teams in their positions
        pair_similarities = []
        for t1, t2 in zip(teams1_list, teams2_list):
            similarity, is_match = compare_teams(t1, t2)
            if not is_match:
                return 0.0, False
            pair_similarities.append(similarity)

        return min(pair_similarities), True

    def validate_group_consistency(entries, new_teams, new_time, new_bookmaker):
        """
        Validate that a new entry is consistent with ALL existing entries in a group
        Returns True only if the new entry matches ALL existing entries
        """
        if not entries:
            return False

        # Create set of existing bookmakers
        existing_bookmakers = set()

        for entry in entries:
            # Extract bookmaker from existing entry
            existing_bookmaker = entry[2].get("bookmaker")

            # Check for duplicate bookmaker
            if existing_bookmaker == new_bookmaker:
                logger.info(f"Duplicate bookmaker found: {new_bookmaker}")
                return False

            existing_bookmakers.add(existing_bookmaker)

            existing_teams = list(entry[2].get("teams", {}).keys())[0]
            existing_time = entry[2].get("start_time")
            if not is_time_similar(new_time, existing_time, period=period):
                logger.info(
                    f"Times not similar for {period} period: {new_time} vs {existing_time}"
                )
                return False
            similarity, is_valid = compare_team_names(new_teams, existing_teams)
            if not is_valid:
                logger.info(
                    f"Group validation failed - New teams: {new_teams} "
                    f"Existing teams: {existing_teams} "
                    f"Similarity: {similarity:.4f}"
                )
                return False
        return True

    # helper function to find matching time group
    def find_matching_time_group(current_time, grouped_times, period):
        """Find matching time group considering period-specific rules"""
        if period == "upcoming":
            return current_time  # Exact match for upcoming

        # For live matches, check within ±25 minute buffer
        for existing_time in grouped_times:
            if is_time_similar(current_time, existing_time, period=period):
                logger.debug(
                    f"Found matching time group: {existing_time} for time: {current_time}"
                )
                return existing_time

        return current_time  # No matching group found, use current time

    for index in group:
        try:
            data_key, path = index_mapping[index]
            logger.info(
                f"Processing index {index} - Data key: {data_key}, Path: {path}"
            )
            json_obj = redis_db.json().get(data_key, Path(f"$.[{path}]"))[0]
            # Extract start_time from the JSON object
            start_time = json_obj.get("start_time")

            current_teams = list(json_obj.get("teams", {}).keys())[0]
            logger.info(f"Extracted teams from current entry: {current_teams}")

            # Find appropriate time group based on period
            target_time = find_matching_time_group(
                start_time, grouped_by_start_time.keys(), period
            )
            # Check if this entry should be grouped with existing entries
            matching_group_id = None
            for group_id, entries in grouped_by_start_time[target_time].items():
                logger.debug(f"Checking against group with id: {group_id}")
                # Only match if current teams match ALL teams in the group
                if validate_group_consistency(
                    entries, current_teams, start_time, json_obj.get("bookmaker")
                ):
                    matching_group_id = group_id
                    logger.info(f"Found matching subgroup: id -> {group_id}")
                    break

            # Add to matching subgroup or create new one
            if matching_group_id:
                target_group = matching_group_id
            else:
                target_group = generate_match_id()
                logger.info(f"Creating new subgroup: {target_group}")

            grouped_by_start_time[target_time][target_group].append(
                (data_key, path, json_obj)
            )
            logger.info(
                f"Added to group with start time: {target_time}, nested under target group -> {target_group}"
            )
            logger.debug(
                f"Group now contains {len(grouped_by_start_time[target_time][target_group])} entries"
            )

        except Exception as e:
            logger.warning(f"Error processing group item: {e}")
            continue
    logger.info(f"Processing {len(grouped_by_start_time)} groups for Redis updates")
    for start_time, subgroups in grouped_by_start_time.items():
        for group_id, entries in subgroups.items():
            logger.info(
                f"Processing group with start time {start_time}, containing {len(entries)} entries"
            )
            if len(entries) >= least_count:
                try:
                    market_type = market  # Extract THREE_WAY or BTTS from stream name
                    canonical_info = get_canonical_names(entries)
                    logger.info(
                        f"Generated canonical info for group {group_id}: {canonical_info}"
                    )
                    match_key = f"matched_teams_{stream_name}:{group_id}"
                    team_names = []
                    match_date = None
                    match_team_objects = {}
                    for data_key, path, json_obj in entries:
                        bookmaker = json_obj.get("bookmaker")
                        match_date = json_obj.get("target_date")
                        team_name = list(json_obj.get("teams", {}).keys())[0]
                        match_team_objects[bookmaker] = json_obj
                        team_names.append(team_name)
                        logger.debug(
                            f"Added team {team_name} from bookmaker {bookmaker}"
                        )
                    team_names_str = ";".join(team_names)
                    logger.info(f"Final team names string: {team_names_str}")
                    redis_db.json().set(
                        match_key,
                        Path.root_path(),
                        {
                            "match_team_objects": match_team_objects,
                            "teams": team_names_str,
                            "created": get_current_date(),
                            "arbitrage": 0,
                            "arb_updated": 0,
                            "bookie_updated": 0,
                            "match_date": match_date,
                            "start_time": start_time,
                            "canonical_info": {
                                "competition": {
                                    "name": canonical_info["competition"]["name"],
                                    "country": canonical_info["competition"]["country"],
                                },
                                "teams": {
                                    "home": canonical_info["teams"]["home"],
                                    "away": canonical_info["teams"]["away"],
                                },
                                "metadata": {
                                    "source_count": canonical_info["teams"][
                                        "source_count"
                                    ],
                                    "confidence": canonical_info["confidence"],
                                    "group_id": group_id,
                                    "market_type": market_type,
                                },
                            },
                            "arbitrage_opportunities": [],
                        },
                    )
                    # redis_db.expire(match_key, 650)
                    stream_id = redis_db.xadd(stream_key, {"data_key": match_key})
                    logger.info(
                        f"Updated match info in Redis - Key: {match_key}, Teams: {team_names_str}, Match ID: {group_id}, Stream: {stream_key}, Start Time: {start_time}"
                    )
                except Exception as e:
                    logger.critical(
                        f"Error updating Redis for start time {start_time}: {e}"
                    )
