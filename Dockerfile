FROM python:bookworm

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y \
build-essential

RUN make init
