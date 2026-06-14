# INF343 — Laboratorio 2: DiscoPass

> **Nota de flujo de trabajo (Claude):** los errores se pegan únicamente con el prompt
> *"arregla y explícame este prompt"* → diagnosticar causa raíz, arreglar, explicar y
> documentar en `ERRORS_FOUND.md`. Nunca malgastar tokens (sin cortesías de relleno).
> Detalle completo en `CLAUDE.md`.

Sistema distribuido de venta y validación de entradas para discotecas, escrito en
**Go**, donde 5 tipos de procesos se comunican **exclusivamente por gRPC + Protocol
Buffers**, cada uno en su propio contenedor Docker. El diseño imita una base de datos
estilo **DynamoDB** (replicación con quórum N/W/R) coordinada por un único punto central.

---

## Arquitectura global

```
Productores ──gRPC──> ┌─────────┐ ──gRPC──> Nodos DB (DB1, DB2, DB3)
                      │ BROKER  │
Consumidores ─gRPC──> │ central │ ──gRPC──> Banco USM
                      └─────────┘
```

La regla de oro del laboratorio: **el Broker es el ÚNICO autorizado a hablar con los
nodos DB y con el banco.** Productores y consumidores nunca tocan la base de datos
directamente — todo pasa por el broker.

---

## El contrato: `proto/discopass.proto`

Define los mensajes y 3 servicios gRPC. De aquí se autogeneran `discopass.pb.go` y
`discopass_grpc.pb.go` (con `make proto`).

- **Broker** expone: `RegistrarEntidad`, `PublicarEvento`, `ConsultarEventos`,
  `ComprarEntrada`, `ObtenerHistorial`, `SolicitarBacklog`.
- **NodoDB** expone (solo lo usa el broker): `Escribir`, `LeerEventos`,
  `LeerTickets`, `Salud`.
- **Banco** expone (solo lo usa el broker): `ValidarPago`.

Mensaje clave: `Evento` lleva un `evento_id` UUID que habilita la **idempotencia**.

---

## Los 5 componentes

### 1. Broker (`cmd/broker/main.go`) — el cerebro

Mantiene **todo el estado en memoria**, protegido por un único `sync.Mutex` (`s.mu`).
Estado relevante:

- `eventos map[id]*Evento` → log maestro (con stock actual).
- `tickets []*Ticket` → todos los tickets emitidos.
- `comprasUsuarioEvento map[clave]bool` → anti-duplicado.
- Mapas de entidades registradas + contadores para el reporte.

Lógica que implementa:

**Registro/autenticación** (`RegistrarEntidad`): toda entidad debe registrarse
primero; mensajes de no registradas son rechazados.

**Publicación** (`PublicarEvento`):
1. Verifica que la discoteca esté registrada.
2. **Idempotencia**: si el `evento_id` ya existe → descarta.
3. Valida datos (`validarEvento`): categoría en la lista blanca, stock>0, precio>0,
   fechas presentes.
4. Guarda en el log maestro y **escribe replicado N=3 / W=2**.

**Escritura con quórum** (`escribirEnNodos`): lanza una goroutine por cada nodo en
paralelo (timeout 2s), cuenta los ACK. Se considera exitosa con **≥2 confirmaciones
(W=2)** → tolera que 1 nodo esté caído.

**Lectura con quórum** (`leerEventosQuorum`, `stockQuorum`, `leerTicketsQuorum`):
pide a los 3 nodos, exige **≥2 respuestas (R=2)** y **reconcilia** quedándose con la
versión que aparece en más réplicas (`versionMayoritaria`). Esto materializa la
consistencia eventual.

**Compra** (`ComprarEntrada`) — el flujo más delicado:
1. Bloquea el mutex → **serializa las compras** (evita sobreventa y condiciones de
   carrera).
2. Verifica usuario, evento existente y no-duplicado.
3. Lee stock con quórum R=2 (respaldo: log maestro).
4. Llama al banco con **timeout de 3s** (`consultarPago`). Si no responde → compra
   **PENDIENTE**.
5. Si aprueba: genera `ticket_id` único, **descuenta stock**, persiste evento+ticket
   en los nodos.

**Resincronización** (`SolicitarBacklog`): cuando un nodo revive, le entrega el estado
completo (eventos+tickets). Nota: la replicación es **mediada por el broker**, no
nodo-a-nodo.

**Reporte** (`generarReporte`): una goroutine cada 20s escribe `Reporte.txt` con 6
secciones (discotecas, estado de nodos, compras/tickets, pagos, fallos, conclusión).

### 2. Nodo DB (`cmd/dbnode/main.go`) — almacenamiento replicado

Guarda `eventos` y `tickets` en mapas en memoria. Implementa `Escribir`/
`LeerEventos`/`LeerTickets`/`Salud`.

Lo interesante: **simula fallos** (`simularFallos`). Cada cierto tiempo (escalonado por
nodo: DB1 antes, DB3 después) pone `caido=true` por 15-25s; mientras está caído rechaza
todo. Al recuperarse llama a `resincronizar()`, que pide el backlog al broker y
**reemplaza su estado completo**. Así se demuestra tolerancia a fallos + consistencia
eventual: con 1 nodo caído el sistema sigue funcionando gracias a W=2/R=2.

### 3. Banco USM (`cmd/banco/main.go`) — pago probabilístico

`ValidarPago` aprueba con **80% de probabilidad** (90% si `medio_pago=credito`).
Además, con 5% de probabilidad simula un **fallo: duerme 5s y devuelve error** → fuerza
el timeout de 3s del broker, dejando la compra "pendiente".

### 4. Productor (`cmd/productor/main.go`) — las discotecas

Lee un catálogo JSON, filtra los eventos de SU discoteca, se registra y en bucle publica
eventos cada 30-40s. Cada publicación añade variación aleatoria a precio/stock
(`construirEvento`) y a propósito inyecta casos de prueba:

- **20%** reenvía con prefijo `RE-` → prueba idempotencia.
- **8%** usa una categoría inválida (Jazz, Salsa…) → prueba la validación.

### 5. Consumidor (`cmd/consumidor/main.go`) — los clientes

En bucle: consulta la cartelera, elige un evento que no haya comprado, e intenta
comprarlo (timeout 10s). Guarda los tickets aprobados en `usuario_<id>.csv`. Cada 5
compras **simula desconexión/reconexión**: borra su estado local y lo recupera vía
`ObtenerHistorial` (lectura R=2), demostrando recuperación de cliente.

---

## Cómo usarlo

**Con Docker (todo en una máquina):**

```bash
make up      # docker compose up (broker, 3 DB, banco, 4 productores, 2 clientes)
make down    # detener
```

**Distribuido en 4 VMs:**

```bash
make docker-VM1   # Broker
make docker-VM2   # Productores + DB3
make docker-VM3   # Consumidores + DB2
make docker-VM4   # Banco + DB1
```

**Sin Docker (desarrollo rápido):** se levanta cada binario en su terminal (ver
`README.md`). Primero `make build` para compilar; `make proto` solo si se toca el
`.proto`.

**Resultados que produce:**

- `Reporte.txt` (en el dir del broker) — actualizado cada 20s.
- `usuario_ClienteA.csv`, `usuario_ClienteB.csv` — historial de compras.

---

## Comportamiento normal (cómo leer los logs)

Una corrida sana **no muestra errores**: solo el flujo de quórum, caídas y
recuperaciones automáticas. Mapa de las líneas que verás:

**Quórum en acción.** `W=3 ACK` = los 3 nodos vivos confirmaron; `W=2 ACK` = uno
estaba caído y bastaron 2. `consulta cartelera ... (R=2 OK)` = lectura válida con 2
réplicas. Como `W+R > N`, lectura y escritura siempre comparten ≥1 nodo → nunca se lee
un dato viejo, y el servicio tolera **1 nodo caído**.

**Publicar evento** (productor → broker → DBs):
```
db1/db2/db3 ... escritura evento 8eb766a4 OK
broker ... evento 8eb766a4 (Docker Techno Session) almacenado con W=3 ACK
dockersnight ... evento ACEPTADO: Docker Techno Session
```

**Comprar entrada** (cliente → broker → banco → DBs) — secuencia atómica:
```
broker ... consulta cartelera: 38 eventos (R=2 OK)   ← lee catálogo
banco  ... PAGO APROBADO: ... monto=8687             ← cobra (timeout 3s)
db1/db2 ... escritura evento 57f61ab6 OK             ← descuenta stock
db1/db2 ... escritura ticket TK-a09c25c3 OK          ← emite ticket
broker ... compra APROBADA ... stock_restante=206
```
Si el banco responde `PAGO RECHAZADO` → `compra RECHAZADA` y **no se escribe nada**
(o pago+ticket, o nada). Comprar reescribe el evento porque actualiza su `stock_restante`.

**Caída y recuperación** (tolerancia a fallos + consistencia eventual):
```
db2 ... NODO CAIDO por 70 segundos
broker ... nodo db2:50062 no respondio la lectura de eventos   ← sigue con W=2/R=2
...
db2 ... NODO RECUPERADO, solicitando backlog...
broker ... entregando backlog a nodo DB2: 30 eventos, 22 tickets
db2 ... resincronizado: 30 eventos, 22 tickets                 ← queda al día
```

**Idempotencia** (prefijo `RE-` = reenvío): `REENVIO (idempotencia) evento RE-...` →
el broker lo reconoce por su `evento_id` y **no lo duplica**.

**Validación**: `evento INVALIDO (categoria=Jazz)` → `RECHAZO ... categoria invalida`
**antes** de escribir en las DBs.

**Reconexión de cliente**: `simulando desconexion` → `historial recuperado: N tickets`
(reconstruido con lectura R=2 vía `ObtenerHistorial`).

**Cierre**: `Gracefully stopping...` + `Container ... Stopped` **no es un error** — es
`docker compose` apagando los 11 contenedores tras un Ctrl+C.

---

## Resumen de los conceptos distribuidos que demuestra

| Concepto | Dónde |
|---|---|
| Replicación con quórum N=3, W=2, R=2 | `escribirEnNodos`, `*Quorum` en broker |
| Consistencia eventual + reconciliación | `versionMayoritaria` |
| Idempotencia | `evento_id` UUID + chequeo en `PublicarEvento` |
| Tolerancia a fallos | nodos `simularFallos` + W=2/R=2 |
| Resincronización tras caída | `SolicitarBacklog` ↔ `resincronizar` |
| Evitar sobreventa | mutex que serializa `ComprarEntrada` |
| Timeout / degradación elegante | `consultarPago` 3s → "pendiente" |
| Recuperación de cliente | consumidor cada 5 compras → `ObtenerHistorial` |
