import csv
import os
import json

class LinearRegression:
    def __init__(self):
        self.coefficients = []
        self.intercept = 0

    def fit(self, X, y):
        """
        Train a linear regression model.
        """
        n, m = len(X), len(X[0])  # Number of samples and features
        X = [[1] + row for row in X]  # Add intercept column
        XT = [[X[j][i] for j in range(n)] for i in range(m + 1)]  # Transpose X
        XTX = [[sum(XT[i][k] * X[k][j] for k in range(n)) for j in range(m + 1)] for i in range(m + 1)]
        XTy = [sum(XT[i][k] * y[k] for k in range(n)) for i in range(m + 1)]

        # Solve the normal equation (XTX * coefficients = XTy)
        coefficients = [0] * (m + 1)
        for i in range(m + 1):
            coefficients[i] = XTy[i] / (XTX[i][i] if XTX[i][i] != 0 else 1)

        self.intercept = coefficients[0]
        self.coefficients = coefficients[1:]

    def predict(self, X):
        """
        Predict using the linear regression model.
        """
        return [self.intercept + sum(x[i] * self.coefficients[i] for i in range(len(x))) for x in X]

def process_chunk(file_path):
    """
    Map Phase: Process a CSV chunk, extract features, save the preprocessed data, and train a linear regression model.
    """
    import math
    team_encoding = {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8,
                     "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16,
                     "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24,
                     "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30}
    data = {}

    try:
        print(f"Processing file: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            print("File opened successfully. Reading rows...")
            for row in reader:
                try:
                    player = row["PLAYER_NAME"]
                    team = team_encoding.get(row["TEAM_ABBREVIATION"], 0)
                    opponent = team_encoding.get(row["MATCHUP"][-3:], 0)

                    stats = {k: float(row[k]) for k in ["MIN", "REB", "AST", "TOV", "STL", "BLK", "PTS", "NBA_FANTASY_PTS"]}
                    per_minute_stats = {f"{k}_PER_MIN": v / stats["MIN"] if stats["MIN"] > 0 else 0 for k, v in stats.items()}

                    if player not in data:
                        data[player] = {"count": 1, **stats, **per_minute_stats, "TEAM": team, "OPPONENT": opponent}
                    else:
                        data[player]["count"] += 1
                        for k in stats:
                            data[player][k] += stats[k]

                    print(f"Processed row for player: {player}")

                except Exception as e:
                    print(f"Skipping row due to error: {e}")
                    continue

        print(f"Finished reading rows. Total players processed: {len(data)}")

        if not data:
            raise ValueError("No valid data processed from the file.")

        # Prepare training data
        X, y = [], []
        preprocessed_data = []
        print("Preparing training data...")
        for player, stats in data.items():
            try:
                averages = {k: v / stats["count"] for k, v in stats.items() if k not in ["count", "TEAM", "OPPONENT"]}
                X_row = [
                    averages.get(k, 0)
                    for k in ["REB_PER_MIN", "AST_PER_MIN", "STL_PER_MIN", "BLK_PER_MIN", "PTS_PER_MIN"]
                ]
                X_row.extend([stats["TEAM"], stats["OPPONENT"]])
                X.append(X_row)
                y.append(averages.get("NBA_FANTASY_PTS", 0))

                preprocessed_data.append({
                    "PLAYER_NAME": player,
                    **averages,
                    "TEAM": stats["TEAM"],
                    "OPPONENT": stats["OPPONENT"]
                })

                print(f"Prepared data for player: {player}")

            except Exception as e:
                print(f"Error processing player {player}: {e}")
                continue

        if not X or not y:
            raise ValueError("Training data X or y is empty.")

        # Save preprocessed data to file
        preprocessed_file_path = f"preprocessed_{os.path.basename(file_path)}.json"
        with open(preprocessed_file_path, 'w', encoding='utf-8') as f:
            json.dump(preprocessed_data, f, indent=4)
        print(f"Preprocessed data saved to {preprocessed_file_path}")

        print("Training linear regression model...")
        model = LinearRegression()
        model.fit(X, y)
        print("Model trained successfully.")

        output_path = f"map_output_{os.path.basename(file_path)}.json"
        print(f"Saving model parameters to: {output_path}")
        with open(output_path, 'w') as f:
            print(f"Model coefficients: {model.coefficients}")
            print(f"Model intercept: {model.intercept}")
            json.dump({"coefficients": model.coefficients, "intercept": model.intercept}, f)

        print(f"Map output saved to {output_path}")
        return output_path, model, X, y, preprocessed_file_path

    except Exception as e:
        print(f"Error processing chunk: {e}")
        return None


def preprocess_data(file_path, team_encoding):
    """
    Preprocess the dataset to prepare features for training/testing.
    """
    data = []
    targets = []  # Store the fantasy points for testing purposes

    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                team = team_encoding.get(row["TEAM_ABBREVIATION"], 0)
                opponent = team_encoding.get(row["MATCHUP"][-3:], 0)

                stats = {k: float(row[k]) for k in ["MIN", "REB", "AST", "TOV", "STL", "BLK", "PTS"]}
                per_minute_stats = {f"{k}_PER_MIN": v / stats["MIN"] if stats["MIN"] > 0 else 0 for k, v in stats.items()}

                features = {
                    "REB_PER_MIN": per_minute_stats["REB_PER_MIN"],
                    "AST_PER_MIN": per_minute_stats["AST_PER_MIN"],
                    "STL_PER_MIN": per_minute_stats["STL_PER_MIN"],
                    "BLK_PER_MIN": per_minute_stats["BLK_PER_MIN"],
                    "PTS_PER_MIN": per_minute_stats["PTS_PER_MIN"],
                    "TEAM": team,
                    "OPPONENT": opponent
                }
                data.append(list(features.values()))

                if "NBA_FANTASY_PTS" in row:
                    targets.append(float(row["NBA_FANTASY_PTS"]))

            except Exception as e:
                print(f"Error processing row: {e}")
                continue

    return data, targets




def combine_models(map_outputs):
    """
    Reduce Phase: Combine models from the map phase.
    """
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
    """
    Evaluate the model's performance on the test dataset.
    """
    print("\nEvaluating the model...")
    # Predict on the test set
    y_pred = model.predict(X_test)

    # Calculate evaluation metrics
    mse = sum((y_true - y_pred)**2 for y_true, y_pred in zip(y_test, y_pred)) / len(y_test)
    rmse = mse**0.5
    ss_total = sum((y_true - sum(y_test) / len(y_test))**2 for y_true in y_test)
    ss_residual = sum((y_true - y_pred)**2 for y_true, y_pred in zip(y_test, y_pred))
    r2 = 1 - (ss_residual / ss_total if ss_total != 0 else float('inf'))

    # Print metrics
    print(f"Evaluation Metrics - MSE: {mse:.2f}, RMSE: {rmse:.2f}, R2: {r2:.2f}")

    # Display predicted vs actual values
    print("\nPredicted vs Actual Fantasy Points:")
    print(f"{'Actual':<15}{'Predicted':<15}{'Difference':<15}")
    print("="*45)
    for y_true, y_pred in zip(y_test, y_pred):
        diff = abs(y_true - y_pred)
        print(f"{y_true:<15.2f}{y_pred:<15.2f}{diff:<15.2f}")

    return mse, rmse, r2


# Example usage
team_encoding = {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8,
                 "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16,
                 "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24,
                 "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30}

# Example Usage
csv_files = ["nba_game_logs_2022_23.csv", "nba_game_logs_2023_24.csv", "nba_game_logs_2024_25.csv"]

map_results = [process_chunk(file) for file in csv_files]

# Unpack results
map_outputs = []
preprocessed_files = []
models = []
Xs = []
ys = []

for result in map_results:
    if result:
        output_path, model, X, y, preprocessed_path = result
        map_outputs.append(output_path)
        models.append(model)
        Xs.append(X)
        ys.append(y)
        preprocessed_files.append(preprocessed_path)

print("Map Outputs:", map_outputs)
print("Preprocessed Data Files:", preprocessed_files)

if not map_outputs:
    print("No valid map outputs to combine.")
else:
    print("Combining models from map outputs...")
    combined_model = combine_models(map_outputs)
    print("Models combined successfully.")

# Evaluate the last model using its training data for demonstration
if models and Xs and ys:
    test_X, test_y = Xs[-1], ys[-1]
    evaluate_model(models[-1], test_X, test_y)

