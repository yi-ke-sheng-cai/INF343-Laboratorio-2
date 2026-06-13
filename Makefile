.PHONY: all build proto docker-build docker-VM1 docker-VM2 docker-VM3 docker-VM4 up down clean

# Build all Go binaries (without Docker)
build:
	go build ./cmd/broker
	go build ./cmd/dbnode
	go build ./cmd/banco
	go build ./cmd/productor
	go build ./cmd/consumidor

# Regenerate gRPC code from .proto
proto:
	protoc --go_out=. --go_opt=module=discopass \
	       --go-grpc_out=. --go-grpc_opt=module=discopass \
	       proto/discopass.proto

# Build all Docker images
docker-build:
	docker compose build

# VM1: Broker
docker-VM1:
	docker compose up broker

# VM2: Productores + DB3
docker-VM2:
	docker compose up dataclub dockersnight golounge georgiehouse db3

# VM3: Consumidores + DB2
docker-VM3:
	docker compose up cliente_a cliente_b db2

# VM4: Banco + DB1
docker-VM4:
	docker compose up banco db1

# All services in one machine (for testing)
up:
	docker compose up

# Stop all services
down:
	docker compose down

# Clean build artifacts
clean:
	rm -f broker dbnode banco productor consumidor
	rm -rf *.exe
