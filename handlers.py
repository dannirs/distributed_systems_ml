def write_to_file(payload, key):
    with open(key, 'wb') as f:
        file_data_bytes = bytes.fromhex(payload)
        f.write(file_data_bytes)

