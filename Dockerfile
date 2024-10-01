FROM python:3.9-slim
WORKDIR /app
COPY server.py /app
EXPOSE 65432
CMD ["python", "server.py"]