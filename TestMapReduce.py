import csv

encoding_config = {
    "Team": {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8, "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, 
             "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16, "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24, 
             "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30, "FA": 31}, 
    "Pos": {"C": 1, "F": 2, "G": 3}
}

map_config_player = {
    "key_config": "Player",
    "value_config": {
        "Team": "Team",
        "Pos": "Pos",
        "games": "G",
        "minutes": "MIN",
        "points": "PTS",
        "rebounds": "REB",
        "assists": "AST",
        "ft_percentage": "FT%",
        "fg_percentage": "FG%",
        "three_pt_percentage": "3P%",
        "points_per_minute": ("PTS", "MIN", "divide"),
        "rebounds_per_minute": ("REB", "MIN", "divide")
    }
}

map_config_team = {
    "key_config": ("Team", "Pos"),  # Composite key that will be encoded
    "value_config": {
        "player": "Player",
        "team": "Team",  # Ordinal encoding will replace the string value
        "position": "Pos"  # Ordinal encoding will replace the string value
    }
}


reduce_config = {
    "aggregation_config": {
        "Team": "direct",  # Directly take the 'team' field
        "Pos": "direct",  # Directly take the 'position' field
        "minutes": "sum",  # Sum up minutes
        "points": "sum",  # Sum up points
        "games": "sum",
        "rebounds": "sum",  # Sum up rebounds
        "assists": "sum",  # Sum up assists
        "ft_percentage": "average",  # Calculate average free throw percentage
        "fg_percentage": "average",  # Calculate average field goal percentage
        "three_pt_percentage": "average",  # Calculate average three-point percentage
        "points_per_minute": "average",  # Calculate average points per minute
        "rebounds_per_minute": "average"  # Calculate average rebounds per minute
    }
}



def map_extract_key_value(record, key_config, value_config, encoding_config=None):
    """
    Generalized Map function to extract key-value pairs and apply ordinal encodings.

    Args:
        record (dict): A single input record (e.g., player data).
        key_config (str or tuple): Configuration for the key (e.g., "player" or ("team", "position")).
        value_config (dict): Fields to include in the value.
        encoding_config (dict): Configuration for ordinal encodings.

    Returns:
        tuple: Key-value pair (key, value).
    """
    # Extract key
    if isinstance(key_config, tuple):  # Composite key
        key = tuple(
            encoding_config[col].get(record.get(col, None), 0) if col in encoding_config else record.get(col, None)
            for col in key_config
        )
    else:  # Single key
        key = (
            encoding_config[key_config].get(record.get(key_config, None), 0)
            if key_config in encoding_config
            else record.get(key_config, None)
        )

    # Extract or calculate value
    value = {}
    for field, config in value_config.items():
        if isinstance(config, str):  # Direct mapping
            value[field] = record.get(config, None)
        elif isinstance(config, tuple):  # Calculated field (e.g., averages)
            col1, col2, operation = config
            val1 = float(record.get(col1, 0))
            val2 = float(record.get(col2, 1))  # Default to 1 to avoid division by zero
            if operation == "divide" and val2 != 0:
                value[field] = val1 / val2
            elif operation == "add":
                value[field] = val1 + val2
            elif operation == "subtract":
                value[field] = val1 - val2
            elif operation == "multiply":
                value[field] = val1 * val2

    # Apply ordinal encodings to value
    if encoding_config:
        for col, encoding_map in encoding_config.items():
            if col in value:  # Replace directly in the value dictionary
                value[col] = encoding_map.get(value[col], 0)

    return key, value




def reduce_aggregate(key, values, aggregation_config):
    """
    Generalized Reduce function for aggregating values.

    Args:
        key: The key being reduced.
        values (list): List of values associated with the key.
        aggregation_config (dict): Configuration for how to aggregate fields.
                                   Format: {"field_name": "operation"} (e.g., {"points": "sum"}).

    Returns:
        dict: Aggregated result for the key.
    """
    aggregated = {}
    sums = {}
    counts = {}

    # Initialize fields for sums and counts
    for field, operation in aggregation_config.items():
        if operation in ["sum", "average"]:
            sums[field] = 0
            counts[field] = 0

    # Process values
    for value in values:
        for field, operation in aggregation_config.items():
            if field in value and value[field] is not None:
                if operation == "sum":
                    sums[field] += float(value[field])
                elif operation == "average":
                    sums[field] += float(value[field])
                    counts[field] += 1
                elif operation == "direct":
                    # For fields like 'team' or 'position', take the first non-null value
                    if field not in aggregated or aggregated[field] is None:
                        aggregated[field] = value[field]

    # Finalize sums and averages
    for field, operation in aggregation_config.items():
        if operation == "sum":
            aggregated[field] = sums[field]
        elif operation == "average" and counts[field] > 0:
            aggregated[field] = sums[field] / counts[field]

    return aggregated


def reduce_team_position(key, values):
    """
    Reduce function to group unique player names by team-position keys.

    Args:
        key: The (team, position) key.
        values (list): List of dictionaries containing player names.

    Returns:
        list: Unique player names for the team-position key.
    """
    # Use a set to ensure uniqueness
    player_names = {value["player"] for value in values if "player" in value}
    return list(player_names)




def run_mapreduce(data, map_config_player, map_config_team, reduce_config, encoding_config):
    """
    Executes a generalized MapReduce workflow, separating player-level and team-position-level data.

    Args:
        data (list): List of input records.
        map_config_player (dict): Configuration for player-level Map phase.
        map_config_team (dict): Configuration for team-position-level Map phase.
        reduce_config (dict): Configuration for player-level Reduce phase (aggregation logic).
        encoding_config (dict): Configuration for ordinal encodings.

    Returns:
        tuple: Two dictionaries:
               - player_aggregates: Aggregated player-level data.
               - team_position_aggregates: Aggregated team-position-level data.
    """
    # Separate mapped data for player-level and team-position-level aggregation
    mapped_player_data = []
    mapped_team_position_data = []

    for record in data:
        # Map player-level data
        player_key, player_value = map_extract_key_value(
            record, map_config_player["key_config"], map_config_player["value_config"], encoding_config
        )
        mapped_player_data.append((player_key, player_value))

        # Map team-position-level data
        team_position_key, team_position_value = map_extract_key_value(
            record, map_config_team["key_config"], map_config_team["value_config"], encoding_config
        )
        mapped_team_position_data.append((team_position_key, team_position_value))

    # Group data by keys
    grouped_player_data = {}
    for key, value in mapped_player_data:
        if key not in grouped_player_data:
            grouped_player_data[key] = []
        grouped_player_data[key].append(value)

    grouped_team_position_data = {}
    for key, value in mapped_team_position_data:
        if key not in grouped_team_position_data:
            grouped_team_position_data[key] = []
        grouped_team_position_data[key].append(value)

    # Reduce phase
    player_aggregates = {}
    for key, values in grouped_player_data.items():
        player_aggregates[key] = reduce_aggregate(key, values, reduce_config["aggregation_config"])

    team_position_aggregates = {}
    for key, values in grouped_team_position_data.items():
        team_position_aggregates[key] = reduce_team_position(key, values)

    return player_aggregates, team_position_aggregates





def csv_to_dict(csv_file_path):
    """
    Converts a CSV file to a list of dictionaries.

    Args:
        csv_file_path (str): Path to the CSV file.

    Returns:
        list of dict: List of rows as dictionaries.
    """
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def write_to_file(file_path, player_aggregates, team_position_aggregates):
    """
    Write player and team-position aggregates to a file, overwriting it each time.

    Args:
        file_path (str): Path to the output file.
        player_aggregates (dict): Player-level aggregated data.
        team_position_aggregates (dict): Team-position aggregated data.

    Returns:
        None
    """
    with open(file_path, mode='w') as file:
        # Write player-level aggregates
        file.write("Player Aggregates:\n")
        for player, stats in player_aggregates.items():
            file.write(f"{player}: {stats}\n")

        # Write team-position aggregates
        file.write("\nTeam-Position Aggregates:\n")
        for team_position, player_names in team_position_aggregates.items():
            file.write(f"{team_position}: {player_names}\n")

def load_csv(file_path):
    """
    Load a CSV file into a list of dictionaries.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        list of dict: Records from the CSV file.
    """
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


# csv_file_path = "NBA-stats.csv"
# records = csv_to_dict(csv_file_path)

file1_records = load_csv("NBA-stats_2024.csv")
file2_records = load_csv("NBA-stats_2023.csv")

# Combine records
combined_records = file1_records + file2_records

# Player-Level and Team-Position Aggregation
player_aggregates, team_position_aggregates = run_mapreduce(
    combined_records,
    map_config_player,
    map_config_team,
    reduce_config,
    encoding_config
)

# Write results to a file
output_file = "aggregates_output.txt"
write_to_file(output_file, player_aggregates, team_position_aggregates)
