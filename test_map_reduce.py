import csv
import os
import json
import numpy as np

class LinearRegressionModel:
    def __init__(self, combined_model):
        self.coefficients = combined_model["coefficients"]
        self.intercept = combined_model["intercept"]

    def predict(self, X):
        predictions = [
            self.intercept + sum(x[i] * self.coefficients[i] for i in range(len(x)))
            for x in X
        ]
        return [max(0, pred) for pred in predictions]  # predictions is a minimum value of 0

class LinearRegression:
    def __init__(self):
        self.coefficients = []
        self.intercept = 0

    def fit(self, X, y, regularization_factor=0.1):
        n, m = len(X), len(X[0])
        X = [[1] + row for row in X]  # Add bias term
        XT = [[X[j][i] for j in range(n)] for i in range(m + 1)]
        XTX = [[sum(XT[i][k] * X[k][j] for k in range(n)) + (regularization_factor if i == j else 0) for j in range(m + 1)] for i in range(m + 1)]
        XTy = [sum(XT[i][k] * y[k] for k in range(n)) for i in range(m + 1)]
        coefficients = self.solve_system(XTX, XTy)
        self.intercept = coefficients[0]
        self.coefficients = coefficients[1:]

    def solve_system(self, A, b):
        n = len(A)
        for i in range(n):
            pivot = A[i][i]
            for j in range(i, n):
                A[i][j] /= pivot
            b[i] /= pivot
            for k in range(i + 1, n):
                factor = A[k][i]
                for j in range(i, n):
                    A[k][j] -= factor * A[i][j]
                b[k] -= factor * b[i]
        x = [0] * n
        for i in range(n - 1, -1, -1):
            x[i] = b[i] - sum(A[i][j] * x[j] for j in range(i + 1, n))
        return x

    def predict(self, X):
        predictions = [self.intercept + sum(x[i] * self.coefficients[i] for i in range(len(x))) for x in X]
        return [max(0, pred) for pred in predictions]

def process_chunk(file_path, is_training=True):
    team_encoding = {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8,
                     "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16,
                     "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24,
                     "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30}
    data = {}
    player_names = []

    def calculate_advanced_stats(row):
        try:
            fgm, fga, pts, fta, fg3m = map(float, (row["FGM"], row["FGA"], row["PTS"], row["FTA"], row["FG3M"]))
            efg = (fgm + 0.5 * fg3m) / fga if fga > 0 else 0
            ts = pts / (2 * (fga + 0.44 * fta)) if (fga + 0.44 * fta) > 0 else 0
            return efg, ts
        except Exception:
            return 0, 0

    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    player = row["PLAYER_NAME"]
                    if player not in player_names:
                        player_names.append(player)

                    team = team_encoding.get(row["TEAM_ABBREVIATION"], 0)
                    opponent = team_encoding.get(row["MATCHUP"][-3:], 0)
                    is_home = 1 if "vs." in row["MATCHUP"] else 0
                    efg, ts = calculate_advanced_stats(row)

                    stats = {k: float(row[k]) for k in ["MIN", "OREB", "DREB", "AST", "TOV", "STL", "BLK", "PTS"]}
                    stats["REB"] = stats.pop("OREB") + stats.pop("DREB")

                    nba_fantasy_pts = float(row["NBA_FANTASY_PTS"]) if "NBA_FANTASY_PTS" in row else None
                    if nba_fantasy_pts is not None:
                        stats["NBA_FANTASY_PTS"] = nba_fantasy_pts

                    if player not in data:
                        data[player] = {
                            "count": 1,
                            **stats,
                            "TEAM": team,
                            "OPPONENT": opponent,
                            "IS_HOME": is_home,
                            "EFG_TOTAL": efg * stats["MIN"],
                            "TS_TOTAL": ts * stats["MIN"],
                        }
                    else:
                        data[player]["count"] += 1
                        for k in stats:
                            data[player][k] += stats[k]
                        data[player]["IS_HOME"] += is_home
                        data[player]["EFG_TOTAL"] += efg * stats["MIN"]
                        data[player]["TS_TOTAL"] += ts * stats["MIN"]
                except Exception as e:
                    print(f"Row processing error: {e}")
                    continue

        X, y = [], []
        preprocessed_data = []

        for player, stats in data.items():
            try:
                averages = {
                    k: stats[k] for k in ["MIN", "REB", "AST", "TOV", "STL", "BLK", "PTS"]
                }
                for stat in ["REB", "AST", "TOV", "STL", "BLK", "PTS"]:
                    averages[f"{stat}_PER_MIN"] = averages[stat] / averages["MIN"] if averages["MIN"] > 0 else 0
                averages["EFG"] = stats["EFG_TOTAL"] / stats["MIN"] if stats["MIN"] > 0 else 0
                averages["TS"] = stats["TS_TOTAL"] / stats["MIN"] if stats["MIN"] > 0 else 0
                averages["IS_HOME"] = stats["IS_HOME"] / stats["count"]

                if "NBA_FANTASY_PTS" in stats:
                    averages["AVG_NBA_FANTASY_PTS"] = stats["NBA_FANTASY_PTS"] / stats["count"]

                feature_names = [
                    "REB_PER_MIN", "AST_PER_MIN", "TOV_PER_MIN", "STL_PER_MIN",
                    "BLK_PER_MIN", "PTS_PER_MIN", "EFG", "TS", "TEAM", "OPPONENT", "IS_HOME"
                ]

                X_row = [
                    averages.get(k, 0) for k in feature_names[:8]
                ] + [stats["TEAM"], stats["OPPONENT"], averages["IS_HOME"]]

                X.append(X_row)
                if is_training and "NBA_FANTASY_PTS" in stats:
                    y.append(averages["AVG_NBA_FANTASY_PTS"])  # Use average fantasy points for training
                elif not is_training and "NBA_FANTASY_PTS" in stats:
                    y.append(averages["AVG_NBA_FANTASY_PTS"])  # Use actual fantasy points for test evaluation

                preprocessed_data.append({
                    "PLAYER_NAME": player,
                    **averages,
                    "TEAM": stats["TEAM"],
                    "OPPONENT": stats["OPPONENT"]
                })
            except Exception as e:
                print(f"Player processing error: {e}")
                continue

        preprocessed_file_path = f"preprocessed_{os.path.basename(file_path)}.json"
        with open(preprocessed_file_path, 'w', encoding='utf-8') as f:
            json.dump(preprocessed_data, f, indent=4)

        if is_training:
            model = LinearRegression()
            model.fit(X, y)

            output_path = f"map_output_{os.path.basename(file_path)}.json"
            with open(output_path, 'w') as f:
                json.dump({"coefficients": model.coefficients, "intercept": model.intercept}, f)

            return output_path, model, X, y, preprocessed_file_path, player_names
        else:
            return X, y, preprocessed_file_path, player_names

    except Exception as e:
        print(f"Error processing chunk: {e}")
        return None, None, None, None

def debug_feature_variance(X, y):
    means = [np.mean(col) for col in zip(*X)]
    stds = [np.std(col) for col in zip(*X)]
    print("Feature Means:", means)
    print("Feature Std Deviations:", stds)
    print("Target Mean:", np.mean(y))
    print("Target Std Deviation:", np.std(y))

def combine_models(map_outputs):
    combined_model = {"coefficients": [], "intercept": 0}
    total_data_points = 0

    for output in map_outputs:
        with open(output, 'r') as f:
            model = json.load(f)
            combined_model["coefficients"].append(model["coefficients"])
            combined_model["intercept"] += model["intercept"]
            total_data_points += 1

    combined_model["coefficients"] = [sum(x) / total_data_points for x in zip(*combined_model["coefficients"])]
    combined_model["intercept"] /= total_data_points

    with open("combined_model.json", "w") as f:
        json.dump(combined_model, f)

    return combined_model

def evaluate_model(model, X_test, y_test):
    print("\nEvaluating the model...")

    y_pred = model.predict(X_test)


    mse = sum((y_true - y_pred)**2 for y_true, y_pred in zip(y_test, y_pred)) / len(y_test)
    rmse = mse**0.5
    ss_total = sum((y_true - sum(y_test) / len(y_test))**2 for y_true in y_test)
    ss_residual = sum((y_true - y_pred)**2 for y_true, y_pred in zip(y_test, y_pred))
    r2 = 1 - (ss_residual / ss_total if ss_total != 0 else float('inf'))


    print(f"Evaluation Metrics - MSE: {mse:.2f}, RMSE: {rmse:.2f}, R2: {r2:.2f}")

    print("\nPredicted vs Actual Fantasy Points:")
    print(f"{'Actual':<15}{'Predicted':<15}{'Difference':<15}")
    print("="*45)
    for y_true, y_pred in zip(y_test, y_pred):
        diff = abs(y_true - y_pred)
        print(f"{y_true:<15.2f}{y_pred:<15.2f}{diff:<15.2f}")

    return mse, rmse, r2

def evaluate_model_with_names(model, X_test, y_test, player_names):
    print("\nEvaluating the model...")

    y_pred = model.predict(X_test)

    mse = sum((y_true - y_pred) ** 2 for y_true, y_pred in zip(y_test, y_pred)) / len(y_test)
    rmse = mse**0.5
    ss_total = sum((y_true - sum(y_test) / len(y_test)) ** 2 for y_true in y_test)
    ss_residual = sum((y_true - y_pred) ** 2 for y_true, y_pred in zip(y_test, y_pred))
    r2 = 1 - (ss_residual / ss_total if ss_total != 0 else float('inf'))

    print(f"Evaluation Metrics - MSE: {mse:.2f}, RMSE: {rmse:.2f}, R2: {r2:.2f}")

    print("\nPredicted vs Actual Fantasy Points:")
    print(f"{'Player Name':<20}{'Actual':<15}{'Predicted':<15}{'Difference':<15}")
    print("=" * 65)
    for name, y_true, y_pred in zip(player_names, y_test, y_pred):
        diff = abs(y_true - y_pred)
        print(f"{name:<20}{y_true:<15.2f}{y_pred:<15.2f}{diff:<15.2f}")

    return mse, rmse, r2



team_encoding = {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8,
                 "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16,
                 "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24,
                 "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30}

# csv_files = ["nba_game_logs_2023_24_part1.csv", "nba_game_logs_2023_24_part2.csv", "nba_game_logs_2023_24_part3.csv", "nba_game_logs_2024_25.csv"]

# # Use first two CSV files for training
# training_csv_files = csv_files[:3]
# testing_csv_file = csv_files[3]

# # Training Phase
# map_results = [process_chunk(file, is_training=True) for file in training_csv_files]

# map_outputs = []
# preprocessed_files = []
# models = []
# Xs = []
# ys = []

# for result in map_results:
#     if result:
#         output_path, model, X, y, preprocessed_path, player_names = result
#         map_outputs.append(output_path)
#         models.append(model)
#         Xs.append(X)
#         ys.append(y)
#         preprocessed_files.append(preprocessed_path)

# # Combine Models
# if map_outputs:
#     print("Combining models from map outputs...")
#     combined_model = combine_models(map_outputs)
#     print("Models combined successfully.")

# # Evaluate Combined Model on Training Data (Optional)
# print("\nEvaluating combined model on the last training dataset:")
# evaluate_model(LinearRegressionModel(combined_model), Xs[-1], ys[-1])

testing_csv_file = "nba_game_logs_2024_25.csv"
combined_model = {'coefficients': [8.77009308487783, 53.89316946860845, 11.154876571012416, 11.743058595408115, 25.36294219397058, 31.36322682923388, 2.437651515532386, 7.427856414431349, -0.03615716474523691, -0.04369536417746883, 1.3884004560016068], 'intercept': -7.796222773524683} 
# Process test dataset
test_X, test_y, _, test_player_names = process_chunk(testing_csv_file, is_training=False)

# Evaluate the combined model
if combined_model and test_X and test_y:
    evaluate_model_with_names(LinearRegressionModel(combined_model), test_X, test_y, test_player_names)


# # For testing
# print("\nProcessing and testing on the test dataset...")
# test_X, test_y, _ = process_chunk(testing_csv_file, is_training=False)

# # Evaluate Combined Model on Test Data
# if combined_model and test_X and test_y:
#     print("\nEvaluating combined model on the test dataset:")
#     evaluate_model(LinearRegressionModel(combined_model), test_X, test_y)
# else:
#     print("Test data processing failed or missing labels for evaluation.")
