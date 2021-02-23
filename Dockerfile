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
RUN go build mock_store/store.go 

# Run the app
CMD go run mock_store/store.go 3200

#expose 
EXPOSE 3200/udp