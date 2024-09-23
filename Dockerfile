# Use a base Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the server script to the container
COPY server.py /app

# Expose the port that the server listens on
EXPOSE 65432

# Run the server when the container starts
CMD ["python", "server.py"]
