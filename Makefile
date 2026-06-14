.PHONY: help proto build sync up down logs status fetch deploy clean

.DEFAULT_GOAL := help

# DiscoPass — variante DISTRIBUIDA (4 máquinas reales vía SSH, sin Docker).
# Todos los targets delegan en ./disco (orquestador) salvo proto.

help:
	@echo "DiscoPass distribuido - targets:"
	@echo "  make deploy   build + sync + up   (ciclo completo en las 4 maquinas)"
	@echo "  make build    compilar los 5 binarios a bin/ (linux/amd64)"
	@echo "  make sync     rsync binarios+config+scripts a las 4 maquinas"
	@echo "  make up       arrancar los procesos en las 4 maquinas (ssh, background)"
	@echo "  make down     detener los procesos en las 4 maquinas"
	@echo "  make logs     tail -f de run.log de las 4 maquinas"
	@echo "  make status   procesos vivos por maquina"
	@echo "  make fetch    traer Reporte.txt y usuario_*.csv a resultados/"
	@echo "  make proto    regenerar codigo gRPC desde el .proto"
	@echo "  make clean    borrar bin/ y resultados/"

# Regenerar gRPC (solo si se toca el .proto)
proto:
	protoc --go_out=. --go_opt=module=discopass \
	       --go-grpc_out=. --go-grpc_opt=module=discopass \
	       proto/discopass.proto

build:  ; ./disco build
sync:   ; ./disco sync
up:     ; ./disco up
down:   ; ./disco down
logs:   ; ./disco logs
status: ; ./disco status
fetch:  ; ./disco fetch
deploy: ; ./disco deploy

clean:
	rm -rf bin resultados
