FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn httpx pandas numpy scipy

COPY main.py /app/

EXPOSE 8005

CMD ["python", "main.py"]
