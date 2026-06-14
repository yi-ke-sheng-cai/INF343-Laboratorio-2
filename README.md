# INF343 — Laboratorio 2: DiscoPass (variante distribuida)

Misma aplicación Go + gRPC que `lab2-dockercompose`, pero ejecutándose sobre una
**red cerrada de 4 máquinas físicas** del laboratorio en lugar de contenedores
Docker. El código Go es **idéntico**: todas las direcciones se inyectan por flags
(`-broker`, `-nodos`, `-banco`, `-puerto`), así que la migración es puramente de
orquestación (SSH en vez de `docker compose`).

## Topología (red cerrada)

| Máquina | IP gRPC | Componentes | Puertos |
|---|---|---|---|
| dist057 | 10.35.168.67 | **Broker** (único que habla con DB/Banco) | 50051 |
| dist058 | 10.35.168.68 | DataClub, DockersNight, GoLounge, GeorgieHouse + **DB3** | 50063 |
| dist059 | 10.35.168.69 | ClienteA, ClienteB + **DB2** | 50062 |
| dist060 | 10.35.168.70 | **Banco USM** + **DB1** | 50052, 50061 |

```
Productores(058) ─┐                      ┌─ DB1(060) DB2(059) DB3(058)
                  ├─gRPC→ Broker(057) ─gRPC┤
Consumidores(059)─┘                      └─ Banco(060)
```

Toda la topología (IPs, puertos, direcciones derivadas) vive en **`topologia.env`**,
única fuente de verdad que comparten `run.sh` y `disco`.

## Requisito previo: `~/.ssh/config` (en la máquina de control)

El orquestador usa el comando plano `ssh dist0NN` (NO los alias `ssh-0NN`, que no
existen en shells no interactivos). Añade en `~/.ssh/config`:

```
Host dist057 dist058 dist059 dist060
    HostName %h.inf.santiago.usm.cl
    User dist
```

Verifica: `ssh dist057 hostname` debe responder sin pedir el usuario.

## Uso

Dos modos, intercambiables (comparten `topologia.env`):

### Modo A — Demo en vivo (4 terminales, una por máquina)

Logs a color en tiempo real, separados por máquina. Primero despliega los binarios:

```bash
make build && make sync       # compila a bin/ y rsync a las 4 máquinas
```

Luego, en **4 terminales** distintas:

```bash
ssh dist057   # → cd discopass && ./run.sh dist057     (Broker + banner)
ssh dist058   # → cd discopass && ./run.sh dist058     (4 productores + DB3)
ssh dist059   # → cd discopass && ./run.sh dist059     (2 consumidores + DB2)
ssh dist060   # → cd discopass && ./run.sh dist060     (Banco + DB1)
```

Arranca dist057 primero por claridad (no es obligatorio: cada componente reintenta
el registro hasta que el broker está vivo). `Ctrl+C` en una terminal apaga todos
los procesos de esa máquina.

### Modo B — Orquestador `disco` (el "programa principal")

Un comando levanta/apaga todo desde la máquina de control vía SSH:

```bash
make deploy     # build + sync + up  (lanza las 4 máquinas en background)
make logs       # tail -f de las 4 (prefijado por máquina)
make status     # procesos vivos por máquina
make fetch      # trae Reporte.txt y usuario_*.csv a ./resultados/
make down       # detiene los procesos en las 4 máquinas
```

Equivalentes con el script directo: `./disco {build|sync|up|down|logs [maq]|status|fetch|deploy}`.

## Qué observar (validación distribuida)

- **Quórum N=3/W=2/R=2**: `PublicarEvento` → `W≥2 ACK`; `ConsultarEventos (R=2 OK)`.
- **Tolerancia a fallos**: mata un DB → `make down` no; usa
  `ssh dist058 pkill -f dbnode`. El broker sigue con W=2/R=2; al relanzar, el nodo
  pide backlog y se **resincroniza**.
- **Regla de oro**: solo el broker dialoga con DB/Banco — las IPs de DB/Banco solo
  aparecen en los flags `-nodos`/`-banco` del broker (dist057), nunca en productores
  ni consumidores.

## Resultados

- `Reporte.txt` (dist057, cada 20s) y `usuario_ClienteA/B.csv` (dist059).
  `make fetch` los copia a `./resultados/` en la máquina de control.

## Configuración

Para cambiar IPs, puertos o el directorio remoto, edita **`topologia.env`**.
No hay direcciones hardcodeadas en el código Go ni en los scripts.
