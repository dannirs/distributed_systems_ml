from nba_api.stats.endpoints import playergamelogs
import pandas as pd

# Fetch game logs for the entire league
player_logs = playergamelogs.PlayerGameLogs(
    season_nullable='2024-25',
    season_type_nullable='Regular Season'
)

# Convert to a DataFrame
game_logs_df = player_logs.get_data_frames()[0]

# Save to CSV
game_logs_df.to_csv('nba_game_logs_2024_25.csv', index=False)
