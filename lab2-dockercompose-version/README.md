# INF343 — Laboratorio 2: DiscoPass

Sistema distribuido de venta y validación de entradas para discotecas.
Comunicación estricta por **gRPC + Protocol Buffers**.
Cada entidad corre en un contenedor Docker separado.

## Integrantes y roles

- **Broker Central** — coordinación, validación, escritura/lectura distribuida
- **Nodos DB (DB1, DB2, DB3)** — almacenamiento replicado (N=3, W=2, R=2)
- **Banco USM** — servicio de pago probabilístico
- **Productores (DataClub, Dockers Night, GoLounge, GeorgieHouse)** — generan eventos
- **Consumidores (ClienteA, ClienteB)** — compran entradas

## Arquitectura

```
Productores ──gRPC──> Broker ──gRPC──> Nodos DB (DB1, DB2, DB3)
Consumidores ──gRPC──> Broker ──gRPC──> Banco USM
```

- **Broker** = único punto de coordinación (registro, validación, idempotencia,
  escritura N=3/W=2, lectura R=2, resincronización de nodos caídos, reporte final).
- **Nodos DB**: replicación tipo DynamoDB con consistencia eventual.
  Fallo simulado interno (caída temporal + resincronización vía `SolicitarBacklog`).
- **Banco USM**: aprueba pagos con 80% probabilidad (90% si medio_pago=credito).
  Timeout de 3s en el broker → compra queda "pendiente" si el banco no responde.
- **Resincronización mediada por broker**: cuando un nodo revive, pide el backlog
  al broker (no replica nodo-a-nodo), respetando la regla de que el broker es el
  único punto autorizado.

## Cómo ejecutar

### Opción 1: Todo en una máquina (pruebas)

```bash
make up
```

### Opción 2: Distribuido en 4 VMs

```bash
# VM1: Broker
make docker-VM1

# VM2: Productores + DB3
make docker-VM2

# VM3: Consumidores + DB2
make docker-VM3

# VM4: Banco + DB1
make docker-VM4
```

### Opción 3: Sin Docker (desarrollo rápido)

```bash
# Terminal 1
go run ./cmd/broker -puerto 50051 -nodos localhost:50061,localhost:50062,localhost:50063 -banco localhost:50052

# Terminales 2-4
go run ./cmd/dbnode -id DB1 -puerto 50061 -broker localhost:50051
go run ./cmd/dbnode -id DB2 -puerto 50062 -broker localhost:50051
go run ./cmd/dbnode -id DB3 -puerto 50063 -broker localhost:50051

# Terminal 5
go run ./cmd/banco -puerto 50052 -broker localhost:50051

# Terminal 6
go run ./cmd/productor -discoteca DataClub -broker localhost:50051 -catalogo config/catalogo_30.json

# Terminal 7
go run ./cmd/consumidor -id ClienteA -broker localhost:50051 -medio credito -intervalo 10
```

## Configuración (flags / variables de entorno)

| Flag | Variable | Default | Descripción |
|------|----------|---------|-------------|
| `-puerto` | `PUERTO` | `50051` (broker) | Puerto del servicio |
| `-nodos` | `NODOS` | `localhost:...` | Direcciones de nodos DB (broker) |
| `-banco` | `BANCO` | `localhost:50052` | Dirección del banco (broker) |
| `-broker` | `BROKER` | `localhost:50051` | Dirección del broker |
| `-id` | `ID` | `DB1` / `ClienteA` | Identificador de la entidad |
| `-min` / `-max` | `MIN_INTERVAL` / `MAX_INTERVAL` | `30` / `40` | Intervalo entre publicaciones (productor) |
| `-intervalo` | `INTERVALO` | `10` | Segundos entre compras (consumidor) |
| `-medio` | `MEDIO_PAGO` | `debito` | Medio de pago del consumidor |
| `-catalogo` | `CATALOGO` | `config/catalogo_30.json` | Catálogo de eventos (productor) |
| `-discoteca` | `DISCOTECA` | `DataClub` | Nombre de la discoteca (productor) |
| `-fallos` | `FALLOS` | `true` | Simular fallos temporales |

## Reportes

- **Reporte.txt**: se genera automáticamente en el directorio del broker
  (cada 20s y al finalizar). Contiene:
  1. Resumen de discotecas
  2. Estado de nodos DB
  3. Compras y tickets
  4. Estado del servicio de pago
  5. Fallos y recuperaciones
  6. Conclusión

- **CSV de usuarios**: se generan `usuario_ClienteA.csv` y `usuario_ClienteB.csv`
  con el historial de compras de cada consumidor.
