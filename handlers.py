import base64

def write_to_file(payload, key):
    with open(key, 'wb') as f:
        file_data_bytes = base64.b64decode(payload)
        f.write(file_data_bytes)

