from nba_api.stats.endpoints import playergamelogs
import pandas as pd

# Fetch game logs for the entire league (replace the season_nullable field)
player_logs = playergamelogs.PlayerGameLogs(
    season_nullable='2024-25',
    season_type_nullable='Regular Season'
)

game_logs_df = player_logs.get_data_frames()[0]

game_logs_df.to_csv('nba_game_logs_2024_25.csv', index=False)
