# Use the official Go image
FROM golang:1.20

# Set the working directory inside the container
WORKDIR /app

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code into the container
COPY . .

# Build the Go application
RUN go build -o ipfsd .

# Expose the port that the application listens on
EXPOSE 8080

# Run the application
CMD ["./ipfsd"]
