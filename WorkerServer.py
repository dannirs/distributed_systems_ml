import socket
from message import message
import json
import threading
from handlers import write_to_file 
from jsonrpc import dispatcher, JSONRPCResponseManager
import random
import os
import re
import base64

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

class FileService:
    def __init__(self, server):
        self.server = server

    @dispatcher.add_method
    def map(self, header_list=None, payload=None):
        key = header_list["key"]
        payload_type = header_list["payload_type"]
        print(f"Processing Map task for {key}")
        try:
            output_path, model, X, y = self.process_chunk(header_list, payload)
            self.server.file_store[output_path] = (payload_type, output_path)
            self.send_data_location(output_path)
            return {"status": "success", "player_result_path": output_path}
        except Exception as e:
            print(f"Error in map: {e}")
            return {"status": "failure", "error": str(e)}

    def process_chunk(self, header_list, data_chunk):
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
            
        if isinstance(data_chunk, str):
            try:
                data_chunk.encode('utf-8').decode('utf-8')
            except (UnicodeEncodeError, UnicodeDecodeError):
                print("Payload is not in utf-8 format.")
                return False
            hex_pattern = re.fullmatch(r'[0-9a-fA-F]+', data_chunk)
            if not hex_pattern:
                print("Payload is not in hex format.")
            try:
                data_chunk = json.loads(data_chunk)
            except json.JSONDecodeError as e:
                print(f"Error decoding results: {e}")
                return

        if not isinstance(data_chunk, list):
            raise ValueError(f"Expected a list for results, got {type(data_chunk)}")
        try:
            print("Processing JSON data...")
            for row in data_chunk:
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

            print(f"Finished processing JSON data. Total players processed: {len(data)}")

            if not data:
                raise ValueError("No valid data processed from the JSON.")

            # Prepare training data
            X, y = [], []
            preprocessed_data = []
            print("Preparing training data...")
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
                    if "NBA_FANTASY_PTS" in stats:
                        y.append(averages["AVG_NBA_FANTASY_PTS"])  # Use average fantasy points for training

                    preprocessed_data.append({
                        "PLAYER_NAME": player,
                        **averages,
                        "TEAM": stats["TEAM"],
                        "OPPONENT": stats["OPPONENT"]
                    })
                except Exception as e:
                    print(f"Player processing error: {e}")
                    continue

            preprocessed_file_path = f"preprocessed_{os.path.basename(header_list['key'])}.json"
            with open(preprocessed_file_path, 'w', encoding='utf-8') as f:
                json.dump(preprocessed_data, f, indent=4)
            print(f"Preprocessed data saved to {preprocessed_file_path}")

            if not X or not y:
                raise ValueError("Training data X or y is empty.")

            print("Training linear regression model...")
            model = LinearRegression()
            model.fit(X, y)
            print("Model trained successfully.")

            output_path = f"map_output_{os.path.basename(header_list['key'])}.json"
            print(f"Saving model parameters to: {output_path}")
            with open(output_path, 'w') as f:
                json.dump([{"coefficients": model.coefficients, "intercept": model.intercept}], f)

            print(f"Map output saved to {output_path}")

            return output_path, model, X, y

        except Exception as e:
            print(f"Error processing JSON: {e}")
            return None

    def combine_models(self, payload):
        combined_model = {"coefficients": [], "intercept": 0}
        total_data_points = 0
        try:
            # Ensure payload is a valid JSON object
            if isinstance(payload, str):
                payload = json.loads(payload)

            if not isinstance(payload, list):
                raise ValueError("Payload must be a list of model objects.")

            for model in payload:
                if not isinstance(model, dict):
                    raise ValueError("Each model in the payload must be a dictionary.")
                if "coefficients" not in model or "intercept" not in model:
                    raise KeyError("Each model must contain 'coefficients' and 'intercept'.")

                combined_model["coefficients"].append(model["coefficients"])
                combined_model["intercept"] += model["intercept"]
                total_data_points += 1

            # Average the coefficients and intercepts
            combined_model["coefficients"] = [
                sum(x) / total_data_points for x in zip(*combined_model["coefficients"])
            ]
            combined_model["intercept"] /= total_data_points
            print(combined_model)
            return combined_model

        except Exception as e:
            print(f"Error in combine_models: {e}")
            raise

    @dispatcher.add_method
    def reduce(self, header_list=None, payload=None):
        try:            
            if isinstance(payload, str):
                try:
                    payload = base64.b64decode(payload).decode('utf-8')
                    payload = json.loads(payload)
                except json.JSONDecodeError as e:
                    print(f"Error decoding results: {e}")
                    return

            # Ensure results is now a list
            if not isinstance(payload, list):
                raise ValueError(f"Expected a list for results, got {type(payload)}")
            
            combined_model = self.combine_models(payload)
            # Save combined model
            result_path = "combined_model.json"
            with open(result_path, "w") as f:
                json.dump(combined_model, f)
            
            print(f"Reduce Phase completed. Combined model saved to: {result_path} in {self.server.ip}:{self.server.port}")
            
            return {"status": "success", "result_path": result_path}
        except Exception as e:
            print(f"Error in Reduce Phase: {e}")
            return {"status": "failure", "error": str(e)}

    @dispatcher.add_method
    def send_data(self, header_list=None, payload=None):
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
                response.update(payload[i])
        return response

    @dispatcher.add_method
    def send_data_location(self, key=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
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

    @dispatcher.add_method
    def send_task_to_client(self, header_list=None):
        task_data = header_list["task_data"]
        client_address = header_list["client_address"]
        jsonrpc = "2.0"
        id = random.randint(1, 40000)
        message =   {
                        "jsonrpc": jsonrpc,
                        "method": task_data["method"],
                        "params": task_data,
                        "id": id
                    }
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

    def notify_master(self, clients):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.master_ip, self.master_port))
                registration_data = {
                        "jsonrpc": "2.0",
                        "method": "master.receive_node_request",
                        "params": {
                            "ip": self.ip,
                            "port": self.port, 
                            "clients": clients[1]
                        },
                        "id": 2442
                    }
                s.sendall(json.dumps(registration_data).encode('utf-8'))
                print(f"Successfully notified MasterNode at {self.master_ip}:{self.master_port}")
        except Exception as e:
            print(f"Failed to notify MasterNode: {e}")

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
            while not is_finished:
                # Receive data from the client
                try:
                    data = conn.recv(10240000).decode('utf-8')
                    if not data:
                        break
                    buffer += data
                except Exception as e:
                    print(f"Error receiving data: {e}")
                    break

                if buffer.endswith("\n"):
                    buffer += "\n"  # Ensure complete packets end with `\n`

                packets = buffer.split("\n")  # Split by `\n`
                buffer = packets.pop()  # Keep the last segment in the buffer

                for packet in packets:
                    if not packet.strip():  # Skip empty packets
                        continue
                    try:
                        packet = json.loads(packet)
                        # Extract method from the packet
                        if method is None:
                            method = packet.get("method")
                        if method is not None and method == "reduce" and "header_list" not in packet["params"]:
                            payload_json = base64.b64decode(packet["params"]["payload"]).decode('utf-8')
                            payload = json.loads(payload_json)

                        if method == "send_data":
                            if "header_list" in packet["params"]:
                                payload_type = packet["params"]["header_list"]["payload_type"]
                                if payload_type != 2:
                                    response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
                                    conn.sendall(response.json.encode('utf-8'))
                                    return
                            if headers is None:
                                headers = packet.get("params", {}).get("header_list", {})
                                
                        if method == "send_data" and "finished" in packet["params"]:
                            seq_num = packet.get("params", {}).get("seq_num")
                            payload = packet.get("params", {}).get("payload")
                            finished = packet.get("params", {}).get("finished", False)

                            if payload is not None:
                                collected_payloads.append((seq_num, payload))

                            if finished:
                                is_finished = True
                                break
                        
                        # Collect payloads
                        if method in ["map", "reduce"]:
                            params = packet.get("params", {})

                            # Initialize variables
                            payload_hex = None
                            seq_num = None
                            finished = False

                            # Check if "header_list" exists in params
                            if "header_list" in params:
                                header_list = params.get("header_list", {})
                                payload_hex = header_list.get("payload", params.get("payload")) 
                                seq_num = header_list.get("seq_num", params.get("seq_num"))  
                                finished = header_list.get("finished", params.get("finished", False))  

                                if headers is None:
                                    headers = header_list
                            else:
                                payload_hex = params.get("payload")
                                seq_num = params.get("seq_num")
                                finished = params.get("finished", False)

                            if payload_hex:
                                try:
                                    payload_bytes = base64.b64decode(payload_hex)
                                    payload_json = payload_bytes.decode('utf-8')
                                    payload = json.loads(payload_json)
                                    collected_payloads.append((seq_num, payload))
                                except (ValueError, json.JSONDecodeError) as e:
                                    print(f"Error decoding payload: {e}")

                            if finished:
                                is_finished = True
                                break

                        elif method in ["retrieve_data", "send_data_location", "send_task_to_client"]:
                            response = JSONRPCResponseManager.handle(json.dumps(packet), dispatcher)
                            conn.sendall(response.json.encode('utf-8'))
                            return  

                    except json.JSONDecodeError as e:
                        print(f"JSON decoding error: {e}")
                        buffer += packet  # Re-add to buffer if incomplete
                        continue

            # Finalize and send aggregated payload
            if method == "send_data":
                collected_payloads.sort(key=lambda x: x[0])  # Sort by sequence number
                aggregated_payload = ''.join(payload for _, payload in collected_payloads)

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

            if method in ["map", "reduce"] and headers and is_finished:
                    collected_payloads.sort(key=lambda x: x[0])

                    # Combine all decoded payloads into a single list
                    aggregated_payload = []
                    for _, payload in collected_payloads:
                        aggregated_payload.extend(payload)
                    # Create the RPC request for the map method
                    rpc_request = {
                        "jsonrpc": "2.0",
                        "method": method,
                        "params": {
                            "header_list": headers,
                            "payload": aggregated_payload  # Pass combined payload
                        },
                        "id": 1
                    }

                    # Process the RPC request
                    response = JSONRPCResponseManager.handle(json.dumps(rpc_request), dispatcher)
                    conn.sendall(response.json.encode('utf-8'))

        except Exception as e:
            print(f"Exception in handle_client: {e}")
        finally:
            conn.close()

    def start_server(self, clients):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        self.notify_master(clients)
        print(f"TCP JSON-RPC server listening on {self.ip}:{self.port}")
        while True:  
            conn, addr = server_socket.accept()
            print(f"In server, Connected to {addr}")
            try:
                client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                client_thread.start()
            except Exception as e:
                print(f"Error starting thread for {addr}: {e}")

if __name__ == '__main__':
    rpc_server = WorkerServer()
