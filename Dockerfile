#FROM golang:1.14.3-alpine AS build
# Start from the latest golang base image
FROM golang:latest

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy Go Modules dependency requirements file
COPY go.mod .

# Copy Go Modules expected hashes file
COPY go.sum .

# Download dependencies
RUN go mod download

# Copy all the app sources (recursively copies files and directories from the host into the image)
COPY . .

# Build the app
RUN go build mock_store/main.go

# Run the app
CMD go run mock_store/main.go 4002

#expose 
EXPOSE 4002/udp