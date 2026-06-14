ARG ENTIDAD

FROM golang:1.25-alpine AS builder
ARG ENTIDAD
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/bin ./cmd/$ENTIDAD

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/bin .
COPY config/ config/
ENTRYPOINT ["./bin"]
