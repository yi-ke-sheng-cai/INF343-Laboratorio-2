#!/usr/bin/env bash
# run.sh <maquina> — arranca, en FOREGROUND, los procesos que le tocan a UNA
# máquina del ecosistema distribuido DiscoPass. Pensado para correrse dentro de
# la terminal SSH de esa máquina (modo demo: logs a color en vivo).
#
#   uso:  ./run.sh dist057 | dist058 | dist059 | dist060
#
# Si la máquina tiene >1 proceso, se lanzan en background con & y un trap mata
# todo el grupo ante Ctrl+C (equivalente a `docker compose up <subset>`).
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./topologia.env

MAQ="${1:-}"
if [[ -z "$MAQ" ]]; then
  echo "uso: $0 dist057|dist058|dist059|dist060" >&2
  exit 1
fi

# Mata todo el grupo de procesos al recibir Ctrl+C / TERM / salida.
pids=()
trap 'echo; echo "[run.sh] deteniendo procesos de '"$MAQ"'..."; kill 0 2>/dev/null || true' INT TERM EXIT

lanzar() { "$@" & pids+=("$!"); }

case "$MAQ" in
  dist057)  # Broker (único punto que habla con DB y Banco)
    ./scripts/banner.sh || true
    lanzar ./bin/broker -puerto "$P_BROKER" -nodos "$NODOS" -banco "$BANCO_ADDR"
    ;;

  dist058)  # 4 Productores (discotecas) + DB3
    lanzar ./bin/dbnode -id DB3 -puerto "$P_DB3" -broker "$BROKER_ADDR"
    lanzar ./bin/productor -discoteca "DataClub"     -broker "$BROKER_ADDR" -catalogo config/catalogo_30.json
    lanzar ./bin/productor -discoteca "Dockers Night" -broker "$BROKER_ADDR" -catalogo config/catalogo_30.json
    lanzar ./bin/productor -discoteca "GoLounge"     -broker "$BROKER_ADDR" -catalogo config/catalogo_30.json
    lanzar ./bin/productor -discoteca "GeorgieHouse" -broker "$BROKER_ADDR" -catalogo config/catalogo_30.json
    ;;

  dist059)  # 2 Consumidores (clientes) + DB2
    lanzar ./bin/dbnode -id DB2 -puerto "$P_DB2" -broker "$BROKER_ADDR"
    lanzar ./bin/consumidor -id ClienteA -medio credito -intervalo 15 -broker "$BROKER_ADDR"
    lanzar ./bin/consumidor -id ClienteB -medio debito  -intervalo 20 -broker "$BROKER_ADDR"
    ;;

  dist060)  # Banco USM + DB1
    lanzar ./bin/banco  -puerto "$P_BANCO" -broker "$BROKER_ADDR"
    lanzar ./bin/dbnode -id DB1 -puerto "$P_DB1" -broker "$BROKER_ADDR"
    ;;

  *)
    echo "máquina desconocida: $MAQ (use dist057..dist060)" >&2
    exit 1
    ;;
esac

# Espera a todos; Ctrl+C dispara el trap y apaga el grupo.
wait
