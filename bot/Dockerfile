FROM python:3.12-slim

WORKDIR /src
COPY ["./", "./"]

RUN pip install --no-cache-dir requests python-telegram-bot

CMD ["python","main.py"]