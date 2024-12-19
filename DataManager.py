class DataManager:
    def __init__(self):
        # Registry structure: {original_file: [(file_name, location, type)]}
        self.data_registry = {}

    def store_data_location_client(self, file, client_address):
        """
        Store the location of a file or computation result.
        
        Args:
            original_file_name (str): The name of the original file.
            client_address (tuple): The address of the client (IP, port).
            chunked_file_name (str, optional): The name of the chunked file or intermediate result.
            file_type (str, optional): Type of the file (e.g., 'chunk', 'map_output', 'reduce_output').
        
        Returns:
            str: Success message or error.
        """
        print("Storing data: ", self.data_registry)
        if file not in self.data_registry:
            self.data_registry[file] = []

            # Handle unchunked file storage
            # if len(self.data_registry[original_file_name]) == 0:
        file_type = "chunked"
        self.data_registry[file].append((file, client_address, file_type))
            # else:

                # return f"Conflict: {original_file_name} already has chunked entries."

        print("Stored data: ", self.data_registry)
        return "Success"

    def store_data_location(self, original_file_name, client_address, chunked_file_name=None, file_type="chunk"):
        """
        Store the location of a file or computation result.
        
        Args:
            original_file_name (str): The name of the original file.
            client_address (tuple): The address of the client (IP, port).
            chunked_file_name (str, optional): The name of the chunked file or intermediate result.
            file_type (str, optional): Type of the file (e.g., 'chunk', 'map_output', 'reduce_output').
        
        Returns:
            str: Success message or error.
        """
        print("Storing data: ", self.data_registry)
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
            # Directly add map/reduce outputs without ordering
            self.data_registry[original_file_name].append((chunked_file_name, client_address, file_type))

        else:
            # Handle unchunked file storage
            # if len(self.data_registry[original_file_name]) == 0:
            self.data_registry[original_file_name].append((original_file_name, client_address, file_type))
            # else:

                # return f"Conflict: {original_file_name} already has chunked entries."

        print("Stored data: ", self.data_registry)
        return "Success"

    def get_data_location(self, original_file_name, file_type=None):
        """
        Retrieve the location of files or results for a given original file.

        Args:
            original_file_name (str): The name of the original file.
            file_type (str, optional): Filter results by type (e.g., 'chunk', 'map_output').
        
        Returns:
            list: Locations of files or results, or an error message.
        """
        print("Current data locations: ", self.data_registry)
        if original_file_name in self.data_registry:
            if file_type:
                return [entry for entry in self.data_registry[original_file_name] if entry[2] == file_type]
            return self.data_registry[original_file_name]
        return f"Error: {original_file_name} not found."



# class DataManager:
#     def __init__(self):
#         self.data_registry = {}  

#     def store_data_location(self, original_file_name, client_address, chunked_file_name=None):
#         if original_file_name not in self.data_registry:
#             self.data_registry[original_file_name] = []

#         if chunked_file_name:
#             base_name = original_file_name.rsplit('.', 1)[0]
#             try:
#                 if "_part" in chunked_file_name and base_name in chunked_file_name:
#                     chunk_number = int(chunked_file_name.replace(base_name + "_part", "").rsplit('.', 1)[0])
#                 else:
#                     raise ValueError(f"Invalid chunk name format: {chunked_file_name}")
#             except ValueError as e:
#                 return f"Error parsing chunk number: {e}"

#             for idx, (existing_chunk, _) in enumerate(self.data_registry[original_file_name]):
#                 existing_chunk_number = int(
#                     existing_chunk.replace(base_name + "_part", "").rsplit('.', 1)[0]
#                 )
#                 if chunk_number < existing_chunk_number:
#                     self.data_registry[original_file_name].insert(idx, (chunked_file_name, client_address))
#                     return "Success"
#             self.data_registry[original_file_name].append((chunked_file_name, client_address))
#         else:
#             if len(self.data_registry[original_file_name]) == 0:
#                 self.data_registry[original_file_name].append((original_file_name, client_address))
#             else:
#                 return f"Conflict: {original_file_name} already has chunked entries."
#         print("Stored data: ", self.data_registry)
#         return "Success"

#     def get_data_location(self, original_file_name):
#         print("current data locations: ", self.data_registry)
#         if original_file_name in self.data_registry:
#             return self.data_registry[original_file_name]
#         return f"Error: {original_file_name} not found."
