class DataManager:
    def __init__(self):
        self.data_registry = {}  

    def store_data_location(self, original_file_name, client_address, chunked_file_name=None):
        if original_file_name not in self.data_registry:
            self.data_registry[original_file_name] = []

        if chunked_file_name:
            base_name = original_file_name.rsplit('.', 1)[0]
            try:
                if "_part" in chunked_file_name and base_name in chunked_file_name:
                    chunk_number = int(chunked_file_name.replace(base_name + "_part", "").rsplit('.', 1)[0])
                else:
                    raise ValueError(f"Invalid chunk name format: {chunked_file_name}")
            except ValueError as e:
                return f"Error parsing chunk number: {e}"

            for idx, (existing_chunk, _) in enumerate(self.data_registry[original_file_name]):
                existing_chunk_number = int(
                    existing_chunk.replace(base_name + "_part", "").rsplit('.', 1)[0]
                )
                if chunk_number < existing_chunk_number:
                    self.data_registry[original_file_name].insert(idx, (chunked_file_name, client_address))
                    return "Success"
            self.data_registry[original_file_name].append((chunked_file_name, client_address))
        else:
            if len(self.data_registry[original_file_name]) == 0:
                self.data_registry[original_file_name].append((original_file_name, client_address))
            else:
                return f"Conflict: {original_file_name} already has chunked entries."

        return "Success"

    def get_data_location(self, original_file_name):
        if original_file_name in self.data_registry:
            return self.data_registry[original_file_name]
        return f"Error: {original_file_name} not found."
