import socket
from message import message
import json
import threading
from handlers import write_to_file 
from jsonrpc import dispatcher, JSONRPCResponseManager
import time
import random
import os
import csv

# write a script so that instead of the workers being started by MasterNode, use the script to start
# the workers when the configs are located in different directories. Configuration file
# names should be the same.
# dir1 - node1
# dir2 - node2
# dir5 - program files
# script, run with bash so I don't have to type the command line arguments every time
# MapReduce:
# create an interface to the map phase, reduce phase
# rpc call starting from userclient, sending instruction to masternode, masternode
# decomposes instruction to subtask instructions and distributes chunks to worker nodes
# worker node then runs the map call on the data
# at the end, the results of the map should be saved to a file, locally on the worker node
# one worker node probably contains several data chunks; each chunk produces 1 result
# the result is stored on the worker node's local storage
# data_registry - location of the files/keys itself 
# mapreduce_registry - location of the mapreduce computation
# mapreduce_datastore - stores computation itself
# taskID: {orig_file1: [chunk1comp, chunk2comp], orig_file2: [chunk1comp, chunk2comp]}
# reduce phase:
# do all of the reduce computation on only 1 node - simple implementation 
# JobManager picks 1 worker as the worker responsible for the reduce tasks
# JobManager sends instructions to all worker nodes to have them send their map computations
# to the target node 
# JobManager can send an instruction to each worker node to have them send their map
# computation directly to the target node, or MasterNode sends an instruction to the
# target node containing information on location of each map computation, and the target
# node uses the information to get files from the worker node. This can be done in parallel
# or in sequence
# depends which performance is better
# test using data from hadoop, etc. 
# tree: 2    tree: 3
# tree: 5
# only 1 target node, so we skip shuffle 
# after the reduce task, the result is stored locally on the target node
# the location of the result is sent to MasterNode's DataManager
# for now implement 1 phase mapreduce, can also implement multiple phase mapreduce later
# k-means is multiple mapreduce
# test mapreduce procedure using multiple nodes, use simple word count example and other examples
# consider fault tolerance; client is not available during map --> rerun task 
# server not available and data is lost --> return all tasks on that node
# can create a replica of each data file/computation chunk and store it on 3 worker nodes (optional implementation, don't have to do it right now)

# later on, linear regression can be done in just 1 phase
# ML program 
# Test with the actual data
# Deploy on cloud (can test it out if I have time) --> simple way is to need 4 images, 1 for each node, just run each container image
# a better way is to create 1 image, and run 4 containers, but each container is different (needs to be configured)

class FileService:
    def __init__(self, server):
        encoding_config = {
            "Team": {"ATL": 1, "BOS": 2, "CHA": 3, "CHI": 4, "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8, "GSW": 9, "HOU": 10, "IND": 11, "LAC": 12, 
                    "LAL": 13, "MEM": 14, "MIA": 15, "MIL": 16, "MIN": 17, "NOP": 18, "NYK": 19, "BKN": 20, "OKC": 21, "ORL": 22, "PHI": 23, "PHO": 24, 
                    "POR": 25, "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30, "FA": 31}, 
            "Pos": {"C": 1, "F": 2, "G": 3}
        }

        scoring_config = {
            "points": 1,
            "rebounds": 1.2,
            "assists": 1.5,
            "steals": 3,
            "blocks": 3,
            "turnovers": -1
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
                "steals": "STL",
                "blocks": "BLK",
                "turnovers": "TO",
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
                "steals": "sum",
                "blocks": "sum",
                "turnovers": "sum",
                "ft_percentage": "average",  # Calculate average free throw percentage
                "fg_percentage": "average",  # Calculate average field goal percentage
                "three_pt_percentage": "average",  # Calculate average three-point percentage
                "points_per_minute": "average",  # Calculate average points per minute
                "rebounds_per_minute": "average"  # Calculate average rebounds per minute
            }
        }
        self.server = server
        self.map_config_player = map_config_player  # Import or define map config
        self.reduce_config = reduce_config          # Import or define reduce config
        self.encoding_config = encoding_config      # Import or define encoding config
        self.map_config_team = map_config_team

    @dispatcher.add_method
    def map(self, header_list=None, payload=None):
        key = header_list["key"]
        data_chunk = self.load_file(key)  # Load file (JSON/CSV)

        # Filter players by teams playing
        data_chunk = [
            record for record in data_chunk if record["Team"] in teams_playing
        ]

        player_results = []
        training_data = []
        target_data = []

        for record in data_chunk:
            # Original map logic
            player_key, player_value = self.map_extract_key_value(
                record,
                self.map_config_player["key_config"],
                self.map_config_player["value_config"],
                self.encoding_config
            )

            # Calculate fantasy points
            fantasy_points = sum(
                scoring_config[field] * float(player_value.get(field, 0))
                for field in scoring_config
            )
            player_value["fantasy_points"] = fantasy_points

            # Extract player salary
            player_name = record.get("Player")
            player_value["salary"] = salary_config.get(player_name, 0)

            # Prepare data for linear regression
            feature_vector = [
                player_value.get("games", 0),
                player_value.get("minutes", 0),
                player_value.get("points", 0),
                player_value.get("rebounds", 0),
                player_value.get("assists", 0),
            ]
            training_data.append(feature_vector)
            target_data.append(fantasy_points)

            player_results.append((player_key, player_value))

        # Train linear regression model on the chunk
        lr_model = self.train_linear_regression(training_data, target_data)

        # Save model and results
        result_path = f"map_result_{os.path.basename(key)}.json"
        model_path = f"lr_model_{os.path.basename(key)}.json"
        with open(result_path, "w") as f:
            json.dump(player_results, f)
        with open(model_path, "w") as f:
            json.dump(lr_model, f)  # Save model coefficients/intercept

        return {"status": "success", "result_path": result_path, "model_path": model_path}

    def train_linear_regression(self, X, y):
        # Gradient Descent Parameters
        learning_rate = 0.01
        num_iterations = 1000
        num_features = len(X[0])

        # Initialize weights and bias
        weights = [0.0] * num_features
        bias = 0.0

        # Gradient Descent
        for _ in range(num_iterations):
            weight_gradients = [0.0] * num_features
            bias_gradient = 0.0

            for i in range(len(X)):
                prediction = sum(w * x for w, x in zip(weights, X[i])) + bias
                error = prediction - y[i]

                for j in range(num_features):
                    weight_gradients[j] += error * X[i][j]
                bias_gradient += error

            weights = [w - learning_rate * grad / len(X) for w, grad in zip(weights, weight_gradients)]
            bias -= learning_rate * bias_gradient / len(X)

        return {"weights": weights, "bias": bias}


    @dispatcher.add_method
    def map(self,  header_list=None, payload=None):
        """
        Execute the Map function for the given file chunk and process both player and team levels.

        Args:
            file_chunk (str): Path to the file chunk to process.

        Returns:
            dict: Status and paths to the output files.
        """
        key = header_list["key"]
        payload_type = header_list["payload_type"]
        print(f"Processing Map task for {key} at both player and team levels")

        # Detect file type based on extension
        _, file_extension = os.path.splitext(key)
        file_extension = file_extension.lower()

        # Initialize data chunk
        data_chunk = []

        try:
            if file_extension == ".csv":
                # Load data from a CSV file
                with open(key, 'r') as chunk_file:
                    csv_reader = csv.DictReader(chunk_file)
                    data_chunk = [row for row in csv_reader]

            elif file_extension == ".json":
                # Load data from a JSON file
                with open(key, 'r') as chunk_file:
                    data_chunk = json.loads(chunk_file)  # Assuming the JSON file is a list of records

            else:
                raise ValueError(f"Unsupported file type: {file_extension}")

        except Exception as e:
            print(f"Error loading file {key}: {e}")
            return {"status": "failure", "error": str(e)}

        # Initialize result containers
        player_results = []
        team_results = []

        # Process each record for both configurations
        for record in data_chunk:
            # Player-Level Aggregation
            player_key, player_value = self.map_extract_key_value(
                record,
                self.map_config_player["key_config"],
                self.map_config_player["value_config"],
                self.encoding_config
            )
            player_results.append((player_key, player_value))

            # Team-Level Aggregation
            team_key, team_value = self.map_extract_key_value(
                record,
                self.map_config_team["key_config"],
                self.map_config_team["value_config"],
                self.encoding_config
            )
            team_results.append((team_key, team_value))

        # Save results locally for both levels
        player_result_path = f"map_result_player_{os.path.basename(key)}.json"
        team_result_path = f"map_result_team_{os.path.basename(key)}.json"

        with open(player_result_path, "w") as f:
            json.dump(player_results, f)
        with open(team_result_path, "w") as f:
            json.dump(team_results, f)
        self.send_data_location(player_result_path)
        self.send_data_location(team_result_path)
        self.server.file_store[player_result_path] = (payload_type, player_result_path)
        self.server.file_store[team_result_path] = (payload_type, team_result_path)
        return {
            "status": "success",
            "player_result_path": player_result_path,
            "team_result_path": team_result_path
        }

    def reduce(self, header_list=None, payload=None):
        try:
            # Get the payload from params
            results = payload
            # print(f"Initial results: {results}")

            # Decode payload if it's a hex string
            if isinstance(results, str):
                print("IS STRING")
                try:
                    # If results is a single string, decode it as JSON
                    results = bytes.fromhex(results).decode('utf-8')
                    results = json.loads(results)
                    # print(f"Decoded results: {results}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding results: {e}")
                    return

            # Ensure results is now a list
            if not isinstance(results, list):
                raise ValueError(f"Expected a list for results, got {type(results)}")

            grouped_player_data = {}
            for result in results:
                key, value = result
                if key not in grouped_player_data:
                    grouped_player_data[key] = []
                grouped_player_data[key].append(value)

            reduced_player_results = {}
            for key, values in grouped_player_data.items():
                reduced_player_results[key] = self.reduce_aggregate(key, values, self.reduce_config["aggregation_config"])

            print(f"Reduced data: {reduced_player_results}")
            result_path = "reduce_result.json"
            with open(result_path, "w") as f:
                json.dump(reduced_player_results, f)
            return reduced_player_results

        except Exception as e:
            print(f"Error in reduce: {e}")
            raise


    def map_extract_key_value(self, record, key_config, value_config, encoding_config=None):
        """
        Extract key-value pairs based on map configuration.
        """
        key = self._extract_key(record, key_config, encoding_config)
        value = self._extract_value(record, value_config, encoding_config)
        return key, value

    def _extract_key(self, record, key_config, encoding_config):
        if isinstance(key_config, tuple):  # Composite key
            return tuple(
                encoding_config[col].get(record.get(col, None), 0) if col in encoding_config else record.get(col, None)
                for col in key_config
            )
        else:  # Single key
            return (
                encoding_config[key_config].get(record.get(key_config, None), 0)
                if key_config in encoding_config
                else record.get(key_config, None)
            )

    def _extract_value(self, record, value_config, encoding_config):
        value = {}
        for field, config in value_config.items():
            if isinstance(config, str):  # Direct mapping
                value[field] = record.get(config, None)
            elif isinstance(config, tuple):  # Calculated field
                col1, col2, operation = config
                val1 = float(record.get(col1, 0))
                val2 = float(record.get(col2, 1))  # Default to 1 to avoid division by zero
                if operation == "divide" and val2 != 0:
                    value[field] = val1 / val2
        return value

    def reduce_aggregate(self, key, values, aggregation_config):
        """
        Aggregate values for a given key during the Reduce phase.
        """
        aggregated = {}
        sums = {}
        counts = {}

        # Initialize sums and counts for aggregation
        for field, operation in aggregation_config.items():
            if operation in ["sum", "average"]:
                sums[field] = 0
                counts[field] = 0

        for value in values:
            for field, operation in aggregation_config.items():
                if field in value and value[field] is not None:
                    if operation == "sum":
                        sums[field] += float(value[field])
                    elif operation == "average":
                        sums[field] += float(value[field])
                        counts[field] += 1
                    elif operation == "direct":
                        aggregated[field] = value[field]

        for field, operation in aggregation_config.items():
            if operation == "sum":
                aggregated[field] = sums[field]
            elif operation == "average" and counts[field] > 0:
                aggregated[field] = sums[field] / counts[field]

        return aggregated


    # @dispatcher.add_method
    # def reduce2(self, header_list=None, payload=None):
    #     """
    #     Execute the Reduce function for results.
    #     """
    #     # Deserialize the map_results
    #     print("Running reduce")
    #     print(payload)
    #     try:
    #         map_results = bytes.fromhex(payload).decode('utf-8')
    #         map_results = json.loads(map_results)
    #         print(type(map_results))
    #         # print(map_results)
    #         for result in map_results:
    #             # print(f"Processing entry: {result}, type: {type(result)}")
    #             # Check if result is a valid key-value pair
    #             if isinstance(result, list) and len(result) == 2:
    #                 key, value = result  # Unpack key and value
    #                 # print(f"Valid entry: {key}, {value}")
    #             # else:
    #             #     print(f"Invalid entry detected: {result}")
    #         # map_results = json.loads(map_results)
    #         if isinstance(map_results, list) and len(map_results) == 1 and isinstance(map_results[0], str):
    #             # map_results = json.loads(map_results[0])  # Parse the inner string as JSON
    #             map_results = bytes.fromhex(map_results[0]).decode('utf-8')
    #     except json.JSONDecodeError as e:
    #         raise ValueError(f"Error decoding JSON payload: {e}")

    #     # print("Parsed map results:", map_results)

    #     # Process the map results
    #     grouped_data = {}
    #     for result in map_results:
    #         try:
    #             key, value = result  # Unpack each key-value pair
    #             if key not in grouped_data:
    #                 grouped_data[key] = []
    #             grouped_data[key].append(value)
    #         except ValueError as e:
    #             raise ValueError(f"Error unpacking map result: {result}, {e}")

    #     # Reduce logic
    #     reduced_results = {}
    #     for key, values in grouped_data.items():
    #         reduced_results[key] = self.reduce_aggregate(key, values, self.reduce_config["aggregation_config"])

    #     # Save results
    #     result_path = "reduce_result.json"
    #     with open(result_path, "w") as f:
    #         json.dump(reduced_results, f)

    #     return {"status": "success", "result_path": result_path}


    # @dispatcher.add_method
    # def reduce(self, payload=None):
    #     """
    #     Execute the Reduce function for a single list of results.

    #     Args:
    #         payload (str): A JSON string representing the map results.

    #     Returns:
    #         dict: Status and path to the final reduced result file.
    #     """
    #     # Parse the incoming payload into a Python list
    #     print(payload)
    #     map_results = json.loads(payload)
    #     print("map results:", map_results)
    #     print(f"Processing Reduce task for results")

    #     # Group results by key
    #     grouped_data = {}
    #     for result in map_results:
    #         key, value = result
    #         if key not in grouped_data:
    #             grouped_data[key] = []
    #         grouped_data[key].append(value)

    #     # Reduce the grouped data
    #     reduced_results = {}
    #     for key, values in grouped_data.items():
    #         reduced_results[key] = self.reduce_aggregate(key, values, self.reduce_config["aggregation_config"])

    #     # Save the final results locally
    #     result_path = "reduce_result.json"
    #     with open(result_path, "w") as f:
    #         json.dump(reduced_results, f)

    #     return {
    #         "status": "success",
    #         "result_path": result_path
    #     }



    # @dispatcher.add_method
    # def reduce(self, payload=None):
    #     """
    #     Execute the Reduce function for player and team levels.

    #     Args:
    #         map_results (dict): A dictionary with player and team map results.

    #     Returns:
    #         dict: Status and path to the final reduced result files.
    #     """
    #     map_results = json.loads(payload)
    #     print("map results: ", map_results)
    #     print(f"Processing Reduce task for {len(map_results['player'])} player results and {len(map_results['team'])} team results")

    #     # Group and reduce player-level results
    #     grouped_player_data = {}
    #     for result in map_results["player"]:
    #         key, value = result
    #         if key not in grouped_player_data:
    #             grouped_player_data[key] = []
    #         grouped_player_data[key].append(value)

    #     reduced_player_results = {}
    #     for key, values in grouped_player_data.items():
    #         reduced_player_results[key] = self.reduce_aggregate(key, values, self.reduce_config["aggregation_config"])

    #     # Group and reduce team-level results
    #     grouped_team_data = {}
    #     for result in map_results["team"]:
    #         key, value = result
    #         if key not in grouped_team_data:
    #             grouped_team_data[key] = []
    #         grouped_team_data[key].append(value)

    #     reduced_team_results = {}
    #     for key, values in grouped_team_data.items():
    #         reduced_team_results[key] = self.reduce_aggregate(key, values, self.reduce_config["aggregation_config"])

    #     # Save final results locally
    #     player_result_path = "reduce_result_player.json"
    #     team_result_path = "reduce_result_team.json"

    #     with open(player_result_path, "w") as f:
    #         json.dump(reduced_player_results, f)
    #     with open(team_result_path, "w") as f:
    #         json.dump(reduced_team_results, f)

    #     return {
    #         "status": "success",
    #         "player_result_path": player_result_path,
    #         "team_result_path": team_result_path
    #     }



    @dispatcher.add_method
    def send_data(self, header_list=None, payload=None):
        print("Server is in send_data().")
        print("send_data header: ", header_list)
        print("send_data payload: ", payload)
        payload_type = header_list["payload_type"]
        key = header_list["key"]
        file_path = header_list["file_path"]
        destination_port = header_list["destination_port"]
        source_port = header_list["source_port"]
        if payload_type == 2:
            file_path = f"server_files/{key}"
            write_to_file(payload, file_path)

        self.server.file_store[key] = (payload_type, file_path)
        print(f"'{key}' received and stored in cache.")
        status = 200
        self.send_data_location(key)

        packet = message( 
            method="send_data_resp", 
            source_port=destination_port,
            destination_port=source_port, 
            header_list={"status": status, "key": key}
        )
        headers = packet.process_headers()
        return headers 

    @dispatcher.add_method
    def retrieve_data(self, header_list=None):
        key = header_list["key"]
        destination_port = header_list["destination_port"]
        source_port = header_list["source_port"]
        if key in self.server.file_store:
            payload_type = self.server.file_store[key][0]
            file_path = self.server.file_store[key][1]
            status = 200
        else:
            status = 404
            print(f"'{key}' not found in cache.") 

        payload = ""
        if status == 200 and payload_type == 1: 
            payload = file_path

        packet = message( 
            method="retrieve_data_resp", 
            source_port=destination_port,
            destination_port=source_port,
            header_list={"key": key, "status": status, "payload_type": payload_type, "payload": payload}
        )        
        
        response = packet.process_headers()

        if status == 200 and payload_type == 2:
            payload = packet.process_payload()
            for i in range(len(payload)):
                # print(payload[i])
                # print("retrieve data: ", payload[i]["payload"])
                write_to_file(payload[i]["payload"], "sjlkjflsa.json")
                response.update(payload[i])
        return response


    @dispatcher.add_method
    def send_data_location(self, key=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Server is sending data location")
            s.connect((self.server.master_ip, self.server.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "data.store_data_location",
                "params": {
                    "original_file_name": key,
                    "client_address": (self.server.ip, self.server.port)
                },
                "id": 1
            }
            print("Sending data location: ", request)
            s.sendall(json.dumps(request).encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            print("Data location sent:", response)

    @dispatcher.add_method
    def send_task_to_client(self, header_list=None):
        task_data = header_list["task_data"]
        client_address = header_list["client_address"]
        print(f"WorkerServer received task: {task_data}")
        jsonrpc = "2.0"
        id = random.randint(1, 40000)
        message =   {
                        "jsonrpc": jsonrpc,
                        "method": task_data["method"],
                        "params": task_data,
                        "id": id
                    }
        print(message)
        client = self.server.worker.get_client((client_address[0], client_address[1]))
        client.handle_task(message)

class WorkerServer:
    def __init__(self, master_ip, master_port, ip, port, worker):
        self.file_store = {}
        self.master_ip = master_ip
        self.master_port = master_port
        self.ip = ip
        self.port = port
        self.worker = worker

    # def handle_client(self, conn):
    #     try:
    #         method = ""
    #         file_service = FileService(self)
    #         dispatcher["send_data"] = file_service.send_data
    #         dispatcher["retrieve_data"] = file_service.retrieve_data
    #         dispatcher["send_data_location"] = file_service.send_data_location
    #         dispatcher["send_task_to_client"] = file_service.send_task_to_client
    #         dispatcher["map"] = file_service.map
    #         dispatcher["reduce"] = file_service.reduce

    #         buffer = ""
    #         collected_packets = []
    #         is_reduce_task = False

    #         while True:
    #             data = conn.recv(1024).decode('utf-8')
    #             if not data:
    #                 break

    #             buffer += data

    #             # Split packets if multiple are received
    #             packets = buffer.split("}{")
    #             if len(packets) > 1:
    #                 packets = [
    #                     f"{packet}{{" if i < len(packets) - 1 else f"{packet}}}"
    #                     for i, packet in enumerate(packets)
    #                 ]
    #                 buffer = packets.pop()  # Keep the last (possibly incomplete) packet in the buffer
    #             else:
    #                 packets = [buffer]
    #                 buffer = ""

    #             for packet in packets:
    #                 try:
    #                     parsed_request = json.loads(packet)
    #                 except json.JSONDecodeError:
    #                     # If packet is incomplete, keep it in the buffer
    #                     buffer += packet
    #                     continue

    #                 method = parsed_request.get("method", "")

    #                 if method == "reduce":
    #                     is_reduce_task = True
    #                     payload = parsed_request["params"].get("payload")
    #                     is_last = parsed_request.get("is_last", False)

    #                     if payload:
    #                         # Collect the payload
    #                         collected_packets.append(payload)
    #                         conn.sendall(json.dumps({"status": "ack"}).encode('utf-8'))

    #                     if is_last:
    #                         # Final packet received, process Reduce task
    #                         print("Final packet for Reduce received. Processing...")

    #                         # Call the reduce RPC with all collected packets
    #                         reduce_request = {
    #                             "jsonrpc": "2.0",
    #                             "method": "reduce",
    #                             "params": {"payload": collected_packets},
    #                             "id": parsed_request.get("id")
    #                         }
    #                         response = JSONRPCResponseManager.handle(json.dumps(reduce_request), dispatcher)
    #                         conn.sendall(response.json.encode('utf-8'))
    #                         collected_packets = []  # Reset for future reduce tasks
    #                         return  # Exit loop after Reduce is complete
    #                 else:
    #                     # Handle other methods
    #                     response = JSONRPCResponseManager.handle(packet, dispatcher)
    #                     conn.sendall(response.json.encode('utf-8'))

    #     except Exception as e:
    #         print(f"Exception in handle_client: {e}")
    #     finally:
    #         conn.close()
    #         print("Connection closed")
    def handle_client(self, conn):
        try:
            # Initialize the RPC dispatcher
            file_service = FileService(self)
            dispatcher.update({
                "send_data": file_service.send_data,
                "retrieve_data": file_service.retrieve_data,
                "send_data_location": file_service.send_data_location,
                "send_task_to_client": file_service.send_task_to_client,
                "map": file_service.map,
                "reduce": file_service.reduce,
            })

            # Initialize variables
            buffer = ""
            collected_payloads = []
            headers = None
            method = None
            is_finished = False
            print("handling client")
            while not is_finished:
                print("in while loop")
                # Receive data from the client
                try:
                    data = conn.recv(102400).decode('utf-8')
                    if not data:
                        break
                    print(data)
                    buffer += data.replace('\n', '').replace('\r', '')
                    print("got data")
                except Exception as e:
                    print(f"Error receiving data: {e}")
                    break

                # Split packets using "}{" as the delimiter
                packets = buffer.split("}{")
                if len(packets) > 1:
                    # Re-add braces to make each JSON object valid
                    packets = [
                        f"{packet}}}" if i < len(packets) - 1 else packet
                        for i, packet in enumerate(packets)
                    ]
                    buffer = packets.pop()  # Keep incomplete packet in the buffer
                else:
                    packets = [buffer]
                    buffer = ""

                # Process each complete packet
                for packet in packets:
                    try:
                        # Parse the packet
                        packet = json.loads(packet)

                        # Extract method from the packet
                        if method is None:
                            method = packet.get("method")
                            print("Extracted method:", method)
                        if method is not None and method == "reduce" and "header_list" not in packet["params"]:
                            payload_json = bytes.fromhex(packet["params"]["payload"]).decode('utf-8')
                            payload = json.loads(payload_json)
                            print("DECODED PAYLOAD: ", payload)
                            if isinstance(payload, list):  # Check if the outer structure is a list
                                is_list_of_lists = all(isinstance(item, list) for item in payload)  # Check if all items are lists

                                if is_list_of_lists:
                                    print("The payload is a list of lists.")
                                else:
                                    print("The payload is a list, but not all elements are lists.")
                            else:
                                print("The payload is not a list.")

                        # Handle `send_data` for special processing
                        if method == "send_data":
                            if "header_list" in packet["params"]:
                                payload_type = packet["params"]["header_list"]["payload_type"]
                                if payload_type != 2:
                                    response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
                                    conn.sendall(response.json.encode('utf-8'))
                                    return
                            if headers is None:
                                headers = packet.get("params", {}).get("header_list", {})
                                print("store headers: ", headers)
                                
                        if method == "send_data" and "finished" in packet["params"]:
                            print("processing file")
                            # print(packet)
                            seq_num = packet.get("params", {}).get("seq_num")
                            payload = packet.get("params", {}).get("payload")
                            finished = packet.get("params", {}).get("finished", False)

                            if payload is not None:
                                collected_payloads.append((seq_num, payload))

                            if finished:
                                is_finished = True
                                print("is finished")
                                break
                            
                            # if payload_type == 2:
                            #     payload = conn.recv(1024).decode('utf-8')
                            #     json_payload = json.loads(payload)
                            #     packet["params"].update(json_payload["params"])
                            

                            # response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
                            # conn.sendall(response.json.encode('utf-8'))
                            # return

                        # Handle methods requiring multiple packets (map/reduce)
                        if method in ["map", "reduce"]:
                            if headers is None:
                                headers = packet.get("params", {}).get("header_list", {})

                            payload = packet.get("params", {}).get("payload")
                            seq_num = packet.get("params", {}).get("seq_num")
                            finished = packet.get("params", {}).get("finished", False)
                            # print(f"Received payload (hex): {payload}")
                            if payload is not None:
                                try:
                                    payload_json = bytes.fromhex(packet["params"]["payload"]).decode('utf-8')
                            # payload = json.loads(payload_json)
                                    decoded_payload = json.loads(payload_json)
                                    collected_payloads.append((seq_num, decoded_payload))
                                    if isinstance(decoded_payload, list):  # Check if the outer structure is a list
                                        is_list_of_lists = all(isinstance(item, list) for item in decoded_payload)  # Check if all items are lists

                                        if is_list_of_lists:
                                            print("The payload after decoding is a list of lists.")
                                        else:
                                            print("The payload is a list, but not all elements are lists.")
                                    else:
                                        print("The payload is not a list.")
                                    # decoded_payload = bytes.fromhex(payload).decode('utf-8')
                                    # collected_payloads.append((seq_num, decoded_payload))
                                except ValueError as e:
                                    print(f"Payload decoding error: {e}")

                            if finished:
                                is_finished = True
                                break

                        elif method in ["retrieve_data", "send_data_location", "send_task_to_client"]:
                            response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
                            conn.sendall(response.json.encode('utf-8'))
                            return  # Exit after handling single-packet method

                    except json.JSONDecodeError as e:
                        print(f"JSON decoding error: {e}")
                        buffer += packet  # Re-add to buffer if incomplete
                        continue

            # Finalize and send aggregated payload
            if method == "send_data":
                print("collecting packets")
                print("headers: ", headers)
                collected_payloads.sort(key=lambda x: x[0])  # Sort by sequence number
                aggregated_payload = ''.join(payload for _, payload in collected_payloads)
                # print(aggregated_payload)


                # Create the RPC request
                rpc_request = {
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {
                        "header_list": headers,
                        "payload": aggregated_payload
                    },
                    "id": 1
                }

                response = JSONRPCResponseManager.handle(json.dumps(rpc_request), dispatcher)
                conn.sendall(response.json.encode('utf-8'))

            # Finalize combined payload for map/reduce
            if method in ["map", "reduce"] and headers and is_finished:
                print("processing map")
                if len(collected_payloads) == 1:
                    final_payload = json.dumps(collected_payloads[0])
                else:
                    # Ensure collected_payloads is sorted by sequence number
                    collected_payloads.sort(key=lambda x: x[0])  # Sort by sequence number

                    # Combine all decoded payloads into a single list while maintaining structure
                    final_payload = []
                    for _, payload in collected_payloads:
                        final_payload.extend(payload)  # Extend the aggregated list with each payload

                    # Print to verify the structure
                    print("Aggregated payload (list of lists):", final_payload)

                # Create the RPC request while preserving the list of lists structure
                rpc_request = {
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {
                        "header_list": headers,
                        "payload": final_payload  # Pass the aggregated list directly
                    },
                    "id": 1
                }

                #     collected_payloads.sort(key=lambda x: x[0] if x[0] is not None else -1)
                #     aggregated_payload = [json.loads(payload.replace('\n', '').replace('\r', '')) for _, payload in collected_payloads]
                #     final_payload = json.dumps(aggregated_payload).encode('utf-8').hex()

                # rpc_request = {
                #     "jsonrpc": "2.0",
                #     "method": method,
                #     "params": {
                #         "header_list": headers,
                #         "payload": final_payload
                #     },
                #     "id": 1
                # }
                response = JSONRPCResponseManager.handle(json.dumps(rpc_request), dispatcher)
                conn.sendall(response.json.encode('utf-8'))

        except Exception as e:
            print(f"Exception in handle_client: {e}")
        finally:
            conn.close()
            print("Connection closed")



    # def handle_client(self, conn):
    #     try:
    #         # Initialize the RPC dispatcher
    #         file_service = FileService(self)
    #         dispatcher.update({
    #             "send_data": file_service.send_data,
    #             "retrieve_data": file_service.retrieve_data,
    #             "send_data_location": file_service.send_data_location,
    #             "send_task_to_client": file_service.send_task_to_client,
    #             "map": file_service.map,
    #             "reduce": file_service.reduce,
    #         })

    #         # Initialize variables
    #         buffer = ""
    #         collected_payloads = []
    #         headers = None
    #         method = None
    #         is_finished = False

    #         while not is_finished:
    #             # Receive data from the client
    #             try:
    #                 data = conn.recv(102400).decode('utf-8')
    #                 print("METHOD PACKET: ", data)
    #                 if not data:
    #                     break
    #                 buffer += data
    #             except Exception as e:
    #                 print(f"Error receiving data: {e}")
    #                 break

    #             # Split packets using "}{" as the delimiter
    #             packets = buffer.split("}{")
    #             if len(packets) > 1:
    #                 # Re-add braces to make each JSON object valid
    #                 packets = [
    #                     f"{packet}}}" if i < len(packets) - 1 else packet
    #                     for i, packet in enumerate(packets)
    #                 ]
    #                 buffer = packets.pop()  # Keep incomplete packet in the buffer
    #             else:
    #                 packets = [buffer]
    #                 buffer = ""

    #             # Process each complete packet
    #             for packet in packets:
    #                 try:
    #                     # Parse the packet
    #                     packet = json.loads(packet)
    #                     # print("Parsed packet:", packet)

    #                     # Extract method from the packet
    #                     if method is None:
    #                         method = packet.get("method")
    #                         print("Extracted method:", method)

    #                     # Handle specific methods
    #                     if method in ["send_data", "map", "reduce"]:
    #                         # Extract headers from the first packet
    #                         if headers is None:
    #                             headers = packet.get("params", {}).get("header_list", {})
    #                             print("Extracted headers:", headers)

    #                         # Collect payloads with sequence numbers
    #                         payload = packet.get("params", {}).get("payload")
    #                         seq_num = packet.get("params", {}).get("seq_num")
    #                         finished = packet.get("params", {}).get("finished", False)

    #                         if payload is not None:
    #                             try:
    #                                 decoded_payload = bytes.fromhex(payload).decode('utf-8')
    #                                 # print(f"Decoded payload (seq {seq_num}):", decoded_payload)
    #                                 collected_payloads.append((seq_num, decoded_payload))
    #                                 print("collected payloads: ", len(collected_payloads))
    #                             except ValueError as e:
    #                                 print(f"Payload decoding error: {e}")

    #                         if finished:
    #                             is_finished = True
    #                             break

    #                     elif method in ["retrieve_data", "send_data_location", "send_task_to_client"]:
    #                         # Single-packet methods: Handle immediately
    #                         response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
    #                         conn.sendall(response.json.encode('utf-8'))
    #                         print(f"Response sent for method {method}.")
    #                         return  # Exit after handling single-packet method

    #                 except json.JSONDecodeError as e:
    #                     print(f"JSON decoding error: {e}")
    #                     buffer += packet  # Re-add to buffer if incomplete
    #                     continue

    #         # If the method requires combined payloads, send them to the RPC
    #         if method in ["send_data", "map", "reduce"] and headers and is_finished:
    #             # Sort collected payloads by sequence number if present
    #             aggregated_payload = ""
    #             if collected_payloads:
    #                 print("number of payloads collected: ", len(collected_payloads))
    #                 collected_payloads.sort(key=lambda x: x[0] if x[0] is not None else -1)
    #                 aggregated_payload = ''.join(payload for _, payload in collected_payloads).encode('utf-8').hex()

    #             rpc_request = {
    #                 "jsonrpc": "2.0",
    #                 "method": method,
    #                 "params": {
    #                     "header_list": headers,
    #                     "payload": aggregated_payload
    #                 },
    #                 "id": 1  # ID for JSON-RPC
    #             }
    #             # print("RPC REQUEST: ", rpc_request)
    #             response = JSONRPCResponseManager.handle(json.dumps(rpc_request), dispatcher)
    #             conn.sendall(response.json.encode('utf-8'))
    #             print(f"{method.capitalize()} RPC response sent.")

    #     except Exception as e:
    #         print(f"Exception in handle_client: {e}")
    #     finally:
    #         conn.close()
    #         print("Connection closed")


    # def handle_client(self, conn):
    #     try:
    #         method = ""
    #         file_service = FileService(self)
    #         dispatcher["send_data"] = file_service.send_data
    #         dispatcher["retrieve_data"] = file_service.retrieve_data
    #         dispatcher["send_data_location"] = file_service.send_data_location
    #         dispatcher["send_task_to_client"] = file_service.send_task_to_client
    #         dispatcher["map"] = file_service.map
    #         dispatcher["reduce"] = file_service.reduce

    #         request = conn.recv(1024).decode('utf-8')
    #         print("From client: ", request)
    #         if not request:
    #             return False
    #         print("Server received client's request in handle_client().")
    #         try:
    #             parsed = json.loads(request)
    #             print(type(parsed["params"]))
    #             if isinstance(parsed, dict):
    #                 method = parsed["method"]
    #                 print("method1: ", method)
    #             else: 
    #                 dicts = [json.loads(part) for part in parsed.split("}{")]
    #                 method = dicts[0]["method"]
    #                 print("method2: ", method)
    #         except json.JSONDecodeError as e:
    #             print(f"ERROR: Invalid JSON string: {e}")
    #             pass

    #         if not method: 
    #             # Split packets based on the assumption that each packet is a JSON object
    #             # delimited by `}{` when received together
    #             raw_packets = request.split("}{")
                
    #             # Adjust split packets to form valid JSON strings
    #             if len(raw_packets) > 1:
    #                 packets = [
    #                     f"{packet}{{" if i < len(raw_packets) - 1 else f"{packet}}}"
    #                     for i, packet in enumerate(raw_packets)
    #                 ]
    #             else:
    #                 packets = [buffer]
            
    #             parsed_packet = json.loads(packets[0])
    #             method = parsed_packet.get("method")
            
    #         print("this is the method: ", method)


    #         if not method or method != "send_task_to_client":
    #             print(request)
    #             if method == "reduce":
    #                 try:
    #                     print("in handle_client_reduce")
    #                     buffer = ""
    #                     collected_payloads = []
    #                     is_finished = False
                        
    #                     while not is_finished:
    #                         # Receive packet from the client
    #                         print("reducing")
    #                         data = conn.recv(1024).decode('utf-8')
    #                         print("reduce data: ", data)
    #                         if not data:
    #                             print("no data received")
    #                             break

    #                         buffer += data

    #                         # Split packets if multiple are received
    #                         packets = buffer.split("}{")
    #                         if len(packets) > 1:
    #                             packets = [
    #                                 f"{packet}{{" if i < len(packets) - 1 else f"{packet}}}"
    #                                 for i, packet in enumerate(packets)
    #                             ]
    #                             buffer = packets.pop()  # Keep the last (possibly incomplete) packet in the buffer
    #                         else:
    #                             packets = [buffer]
    #                             buffer = ""

    #                         for packet in packets:
    #                             print("reduce packet: ", packet)
    #                             try:
    #                                 # Parse the JSON-RPC request
    #                                 parsed_request = json.loads(packet)
    #                                 print("reduce packet parsed: ", parsed_request)
    #                             except json.JSONDecodeError:
    #                                 buffer += packet  # Keep in buffer if not complete
    #                                 continue

    #                             # Check the "finished" flag
    #                             finished = parsed_request.get("params", {}).get("finished", False)
    #                             print("finished reduce: ", finished)
    #                             payload = parsed_request.get("params", {}).get("payload")
    #                             print("payload reduce: ", payload)
                                
    #                             print("try to add")
    #                             if payload:
    #                                 print("payload: ", payload)
    #                                 print(type(payload))
    #                                 try:
    #                                     decoded_payload = bytes.fromhex(payload).decode('utf-8')
    #                                     print("Decoded Payload:", decoded_payload)
    #                                     collected_payloads.append(decoded_payload)
    #                                 except ValueError as e:
    #                                     print(f"Payload decoding error: {e}")
                                    
    #                             if finished:
    #                                 is_finished = True
    #                                 break  # Exit packet processing loop
    #                     print("collected payloads: ", collected_payloads)
    #                     # Send all collected payloads to the reduce function
    #                     reduce_request = {
    #                         "jsonrpc": "2.0",
    #                         "method": "reduce",
    #                         "params": {"payload": json.dumps(collected_payloads)},
    #                         "id": 1  # ID for JSON-RPC
    #                     }
    #                     response = JSONRPCResponseManager.handle(json.dumps(reduce_request), dispatcher)
    #                     conn.sendall(response.json.encode('utf-8'))

    #                 except Exception as e:
    #                     print(f"Exception in handle_client: {e}")
    #                 finally:
    #                     conn.close()
    #                     print("Connection closed")
    #                     return

    #             marker = "{\"jsonrpc\":"
    #             marker_index = request.index(marker)
    #             next_marker_index = request.find(marker, marker_index + len(marker))
    #             print(request)
    #             if next_marker_index == -1:
    #                 print("next")
    #                 json_params = json.loads(request)
    #                 print(json_params)
    #                 if json_params['params']['payload_type'] == 2:
    #                     payload = conn.recv(1024).decode('utf-8')
    #                     print(payload)
    #                     json_payload = json.loads(payload)
    #                     print(json_payload)
    #                     json_params['params'].update(json_payload['params'])
    #             else: 
    #                 print("else")
    #                 params = request[marker_index:next_marker_index]
    #                 json_params = json.loads(params)
    #                 payload = request[next_marker_index:]
    #                 json_payload = json.loads(payload)
    #                 json_params['params'].update(json_payload['params'])

    #             request = json.dumps(json_params)
    #         if isinstance(request, dict):
    #             print("DEBUG: Request is a dictionary. Serializing to JSON string.")
    #             request = json.dumps(request)
            
    #         elif isinstance(request, str):
    #             try:
    #                 parsed_request = json.loads(request)
    #                 print("DEBUG: Request is a valid JSON string.")
    #             except json.JSONDecodeError as e:
    #                 print(f"ERROR: Invalid JSON string: {e}")
    #                 return {"error": {"code": -32600, "message": "Invalid Request"}, "id": None, "jsonrpc": "2.0"}

    #         response = JSONRPCResponseManager.handle(request, dispatcher)
    #         print("response: ", response.json)
    #         conn.sendall(response.json.encode('utf-8'))
    #         print("Server generated response.")
    #         time.sleep(0.1)
    #     except Exception as e:
    #         print(f"Exception in handle_client: {e}")
    #     finally:
    #         conn.close()
    #         print("Connection closed")

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"TCP JSON-RPC server listening on {self.ip}:{self.port}")
        while True:  
            print("Waiting for connection...")
            conn, addr = server_socket.accept()
            print(f"In server, Connected to {addr}")
            try:
                client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                client_thread.start()
                print(f"Started thread for connection: {addr}")
            except Exception as e:
                print(f"Error starting thread for {addr}: {e}")

if __name__ == '__main__':
    rpc_server = WorkerServer()
