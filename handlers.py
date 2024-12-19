import base64
import csv

def write_to_file(payload, key):
    with open(key, 'wb') as f:
        file_data_bytes = base64.b64decode(payload)
        f.write(file_data_bytes)

def write_to_file_csv(file_name, data):
    if not data:
        print("No data to write.")
        return

    # Extract headers from the first dictionary
    if isinstance(data[0], dict):
        headers = data[0].keys()
    else:
        print("Data is not in the correct format to determine headers.")
        return

    with open(file_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header row
        writer.writerow(headers)

        # Write the data rows
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row.values())
            else:
                print(f"Skipping invalid row (not a dict): {row}")


def log_data_summary(data, label="Data"):
    if isinstance(data, list):
        print(f"{label} Summary: List with {len(data)} elements")
        if len(data) > 0:
            print(f" - First element type: {type(data[0])}")
            if isinstance(data[0], dict):
                print(f" - First element keys: {list(data[0].keys())}")
    elif isinstance(data, dict):
        print(f"{label} Summary: Dictionary with {len(data)} keys")
        print(f" - Keys: {list(data.keys())}")
    else:
        print(f"{label} Summary: {type(data).__name__}")