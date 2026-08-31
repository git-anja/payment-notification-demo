FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o client-mock .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/client-mock .
CMD ["./client-mock"]
