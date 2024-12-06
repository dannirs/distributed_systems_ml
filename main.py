import json

class FileProcessor:
    def __init__(self, batch_size=10):
        self.batch_size = batch_size

    def check_file_type(self, file_path):
        if file_path.endswith(".json"):
            return "json"
        else:
            raise ValueError("Unsupported file type!")

    def create_packets(self, file_path):
        packets = []
        seq_num = 0

        if self.check_file_type(file_path) == "json":
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)  # Load the JSON content as a list

                print("DEBUG: Data loaded from JSON file:", data)

                # Split data into batches
                for i in range(0, len(data), self.batch_size):
                    batch = data[i:i + self.batch_size]

                    print(f"DEBUG: Batch {seq_num}: {batch}")

                    # Create packet
                    packet = {
                        "seq_num": seq_num,
                        "finished": i + self.batch_size >= len(data),
                        "payload": json.dumps(batch).encode('utf-8').hex()  # Serialize batch
                    }
                    packets.append(packet)
                    seq_num += 1
        else:
            raise ValueError("This test only supports JSON files.")

        return packets


def write_packets_to_file(packets, output_file_path):
    with open(output_file_path, 'w') as file:
        json.dump(packets, file, indent=4)
    print(f"DEBUG: Packets written to {output_file_path}")


def read_packets_from_file(input_file_path):
    with open(input_file_path, 'r') as file:
        packets = json.load(file)

    # print("DEBUG: Packets read from file:", packets)

    # Decode and reconstruct JSON data
    all_batches = []
    for packet in packets:
        decoded_payload = json.loads(bytes.fromhex(packet["payload"]).decode('utf-8'))
        all_batches.extend(decoded_payload)  # Combine batches into one list
        # print(f"DEBUG: Decoded payload from packet {packet['seq_num']}:", decoded_payload)

    return all_batches


def save_reconstructed_json(data, output_json_path):
    with open(output_json_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"DEBUG: Reconstructed JSON saved to {output_json_path}")


def verify_format(data):
    if isinstance(data, list):
        for idx, item in enumerate(data):
            print(f"DEBUG: Item {idx}: {item}")  # Print each item in the list
            if not isinstance(item, list):
                print("ERROR: Data format is incorrect. Found an item that is not a list.")
                print("Problematic item:", item)
                return False
        print("DEBUG: Data format is correct. All items are lists.")
        return True
    else:
        print("ERROR: Data format is incorrect. Top-level structure is not a list.")
        return False


# Test Program
if __name__ == "__main__":
    # Input JSON file
    input_json_path = "map_result_player_NBA-stats_2024_part1.csv.json"

    # Output JSON packets file
    output_packets_path = "packets.json"

    # Reconstructed JSON file
    reconstructed_json_path = "reconstructed_data.json"

    # FileProcessor instance
    processor = FileProcessor(batch_size=3)

    # Step 1: Create packets from the JSON file
    packets = processor.create_packets(input_json_path)

    # Step 2: Write packets to a JSON file
    write_packets_to_file(packets, output_packets_path)

    # Step 3: Read packets from the JSON file and verify structure
    reconstructed_data = read_packets_from_file(output_packets_path)

    # Step 4: Save the reconstructed data back to a JSON file
    save_reconstructed_json(reconstructed_data, reconstructed_json_path)

    # Step 5: Verify format
    format_is_correct = verify_format(reconstructed_data)

    print("\nDEBUG: Reconstructed data:", reconstructed_data)
    print("\nDEBUG: Format is correct:", format_is_correct)
