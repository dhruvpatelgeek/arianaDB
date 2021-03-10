FROM golang:1.15.6-alpine
WORKDIR /src
COPY . .
RUN go build -o dht-server src/main.go
EXPOSE 7262:7292/udp 7262:7292/tcp