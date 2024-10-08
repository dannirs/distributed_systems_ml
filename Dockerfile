FROM python:3.9-slim
WORKDIR /app
COPY . /app
EXPOSE 65432
CMD ["python", "server.py"]