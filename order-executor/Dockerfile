FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn httpx pandas numpy

COPY main.py /app/

EXPOSE 8007

CMD ["python", "main.py"]
