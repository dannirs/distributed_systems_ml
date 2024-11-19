class DataManager:
    def __init__(self):
        self.data_registry = {}  # Stores the mapping of original files to their locations

    def store_data_location(self, original_file_name, client_address, chunked_file_name=None):
        """
        Stores the location of a file or its chunks.

        :param original_file_name: The name of the original file
        :param client_address: The address of the client storing the file
        :param chunked_file_name: The name of the chunked file (optional for non-chunked files)
        :return: Success message or an error message in case of conflict
        """
        if original_file_name not in self.data_registry:
            self.data_registry[original_file_name] = []

        if chunked_file_name:
            # Handle chunked files
            base_name = original_file_name.rsplit('.', 1)[0]
            try:
                if "_part" in chunked_file_name and base_name in chunked_file_name:
                    chunk_number = int(chunked_file_name.replace(base_name + "_part", "").rsplit('.', 1)[0])
                else:
                    raise ValueError(f"Invalid chunk name format: {chunked_file_name}")
            except ValueError as e:
                return f"Error parsing chunk number: {e}"

            # Insert chunk into the correct position
            for idx, (existing_chunk, _) in enumerate(self.data_registry[original_file_name]):
                existing_chunk_number = int(
                    existing_chunk.replace(base_name + "_part", "").rsplit('.', 1)[0]
                )
                if chunk_number < existing_chunk_number:
                    self.data_registry[original_file_name].insert(idx, (chunked_file_name, client_address))
                    return "Success"
            self.data_registry[original_file_name].append((chunked_file_name, client_address))
        else:
            # Handle non-chunked files
            if len(self.data_registry[original_file_name]) == 0:
                self.data_registry[original_file_name].append((original_file_name, client_address))
            else:
                return f"Conflict: {original_file_name} already has chunked entries."

        return "Success"

    def get_data_location(self, original_file_name):
        """
        Retrieves the location of a file or its chunks.

        :param original_file_name: The name of the original file
        :return: List of file locations or an error message if not found
        """
        if original_file_name in self.data_registry:
            return self.data_registry[original_file_name]
        return f"Error: {original_file_name} not found."
