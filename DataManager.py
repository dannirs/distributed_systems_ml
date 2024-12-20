class DataManager:
    def __init__(self):
        # Registry structure: {original_file: [(file_name, location, type)]}
        self.data_registry = {}

    def store_data_location_client(self, file, client_address):
        if file not in self.data_registry:
            self.data_registry[file] = []

        file_type = "chunked"
        self.data_registry[file].append((file, client_address, file_type))
        return "Success"

    def store_data_location(self, original_file_name, client_address, chunked_file_name=None, file_type="chunk"):
        if original_file_name not in self.data_registry:
            self.data_registry[original_file_name] = []

        # Validate and insert chunks in order
        if chunked_file_name and file_type == "chunk":
            base_name = original_file_name.rsplit('.', 1)[0]
            try:
                if "_part" in chunked_file_name and base_name in chunked_file_name:
                    chunk_number = int(chunked_file_name.replace(base_name + "_part", "").rsplit('.', 1)[0])
                else:
                    raise ValueError(f"Invalid chunk name format: {chunked_file_name}")
            except ValueError as e:
                return f"Error parsing chunk number: {e}"

            for idx, (existing_chunk, _, _) in enumerate(self.data_registry[original_file_name]):
                existing_chunk_number = int(
                    existing_chunk.replace(base_name + "_part", "").rsplit('.', 1)[0]
                )
                if chunk_number < existing_chunk_number:
                    self.data_registry[original_file_name].insert(idx, (chunked_file_name, client_address, file_type))
                    return "Success"
            self.data_registry[original_file_name].append((chunked_file_name, client_address, file_type))
        elif chunked_file_name and file_type in ["map_output", "reduce_output"]:
            self.data_registry[original_file_name].append((chunked_file_name, client_address, file_type))
        else:
            self.data_registry[original_file_name].append((original_file_name, client_address, file_type))
        return "Success"

    def get_data_location(self, original_file_name, file_type=None):
        if original_file_name in self.data_registry:
            if file_type:
                return [entry for entry in self.data_registry[original_file_name] if entry[2] == file_type]
            return self.data_registry[original_file_name]
        return f"Error: {original_file_name} not found."
