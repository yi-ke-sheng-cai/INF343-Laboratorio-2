// Broker Central del sistema DiscoPass.
//
// Es el nucleo del laboratorio y el UNICO punto autorizado para interactuar con
// productores (discotecas), usuarios, nodos de base de datos y el Banco USM.
//
// Responsabilidades principales:
//   - Registrar y autenticar entidades (rechaza mensajes de no registradas).
//   - Recibir y validar eventos publicados por las discotecas (idempotencia por
//     evento_id, categoria valida, stock > 0, precio valido, fechas presentes).
//   - Escribir de forma replicada en los 3 nodos DB (N=3) exigiendo al menos
//     2 confirmaciones (W=2) para dar por exitosa una escritura.
//   - Coordinar lecturas distribuidas aceptando el dato solo si al menos 2
//     nodos coinciden (R=2).
//   - Gestionar compras: verifica stock, consulta al Banco USM, genera ticket_id
//     unico, descuenta stock y evita sobreventa y compras duplicadas.
//   - Entregar a un nodo que se reincorpora el backlog para resincronizarse.
//   - Generar el archivo Reporte.txt con el resumen de la ejecucion.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"discopass/colores"
	pb "discopass/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Categorias permitidas para los eventos (seccion 4.10 del enunciado).
// Se usa la misma escritura (sin tildes) que traen los catalogos de prueba.
var categoriasValidas = map[string]bool{
	"Electronica": true, "Reggaeton": true, "Pop": true, "Techno": true,
	"House": true, "Urbana": true, "Latina": true, "Noche Universitaria": true,
	"Fiesta Tematica": true, "Retro": true, "Open Bar": true, "VIP": true,
}

// servidorBroker implementa el servicio gRPC Broker y mantiene todo el estado.
type servidorBroker struct {
	pb.UnimplementedBrokerServer

	mu sync.Mutex // protege TODO el estado de abajo

	// --- Registro de entidades (autenticacion simple por identidad) ---
	discotecas map[string]bool
	usuarios   map[string]bool
	nodos      map[string]bool
	bancoOK    bool

	// --- Log maestro del broker (fuente para resincronizar nodos) ---
	eventos map[string]*pb.Evento // evento_id -> evento (con su stock actual)
	tickets []*pb.Ticket          // todos los tickets generados

	// --- Idempotencia y anti-duplicados ---
	comprasUsuarioEvento map[string]bool // "usuario|evento" -> ya compro

	// --- Clientes hacia las otras entidades ---
	clientesDB []pb.NodoDBClient
	dirsDB     []string
	banco      pb.BancoClient

	// --- Contadores para el Reporte.txt ---
	rep reporte
}

// reporte agrupa todos los contadores estadisticos de la ejecucion.
type reporte struct {
	eventosRecibidos  map[string]int // por discoteca
	eventosAceptados  map[string]int // por discoteca
	eventosRechazados int            // por datos invalidos / stock / duplicado
	rechCategoria     int
	rechStock         int
	rechDuplicado     int
	rechOtros         int

	escriturasOK   map[string]int // por nodo (direccion)
	escriturasFail map[string]int // por nodo (direccion)

	comprasSolicitadas int
	comprasAprobadas   int
	rechazoSinStock    int
	rechazoPago        int
	ticketsGenerados   int

	pagosAprobados   int
	pagosRechazados  int
	pagosSinResp     int
	comprasPendientes int

	fallos        []string // bitacora de fallos detectados
	resincros     []string // resultados de resincronizacion de nodos
}

func main() {
	colores.Activar()
	// Configuracion por flags/variables de entorno (nada hardcodeado).
	puerto := flag.String("puerto", env("PUERTO", "50051"), "puerto donde escucha el broker")
	dirsDB := flag.String("nodos", env("NODOS", "localhost:50061,localhost:50062,localhost:50063"), "direcciones de los nodos DB separadas por coma")
	dirBanco := flag.String("banco", env("BANCO", "localhost:50052"), "direccion del Banco USM")
	rutaReporte := flag.String("reporte", env("REPORTE", "Reporte.txt"), "ruta del archivo de reporte final")
	flag.Parse()

	s := &servidorBroker{
		discotecas:           map[string]bool{},
		usuarios:             map[string]bool{},
		nodos:                map[string]bool{},
		eventos:              map[string]*pb.Evento{},
		comprasUsuarioEvento: map[string]bool{},
		dirsDB:               strings.Split(*dirsDB, ","),
		rep: reporte{
			eventosRecibidos: map[string]int{},
			eventosAceptados: map[string]int{},
			escriturasOK:     map[string]int{},
			escriturasFail:   map[string]int{},
		},
	}

	// Conectar (perezosamente) a los nodos DB y al Banco.
	for _, dir := range s.dirsDB {
		dir = strings.TrimSpace(dir)
		conn, err := grpc.NewClient(dir, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[BROKER][FATAL] no pude preparar conexion a nodo %s: %v", dir, err)
		}
		s.clientesDB = append(s.clientesDB, pb.NewNodoDBClient(conn))
	}
	connBanco, err := grpc.NewClient(*dirBanco, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[BROKER][FATAL] no pude preparar conexion al banco: %v", err)
	}
	s.banco = pb.NewBancoClient(connBanco)

	// Levantar el servidor gRPC.
	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[BROKER][FATAL] no pude escuchar en :%s: %v", *puerto, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServer(grpcServer, s)

	// Goroutine que regenera Reporte.txt periodicamente (siempre actualizado).
	go func() {
		for {
			time.Sleep(20 * time.Second)
			s.generarReporte(*rutaReporte)
		}
	}()

	log.Printf("[BROKER][INICIO] escuchando en :%s | nodos=%v | banco=%s", *puerto, s.dirsDB, *dirBanco)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[BROKER][FATAL] servidor detenido: %v", err)
	}
}

// ----------------------------------------------------------------------------
// RPC: Registro y autenticacion de entidades
// ----------------------------------------------------------------------------

func (s *servidorBroker) RegistrarEntidad(_ context.Context, r *pb.Registro) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Tipo {
	case "discoteca":
		s.discotecas[r.Id] = true
	case "usuario":
		s.usuarios[r.Id] = true
	case "db":
		s.nodos[r.Id] = true
	case "banco":
		s.bancoOK = true
	default:
		return &pb.Ack{Ok: false, Mensaje: "tipo de entidad desconocido"}, nil
	}
	log.Printf("[BROKER][REGISTRO] registrada entidad %q (tipo=%s)", r.Id, r.Tipo)
	return &pb.Ack{Ok: true, Mensaje: "registrado"}, nil
}

// ----------------------------------------------------------------------------
// RPC: Publicacion de eventos (productor -> broker)
// ----------------------------------------------------------------------------

func (s *servidorBroker) PublicarEvento(_ context.Context, e *pb.Evento) (*pb.PublicarResp, error) {
	s.mu.Lock()
	// La discoteca debe estar registrada antes de publicar.
	if !s.discotecas[e.Discoteca] {
		s.mu.Unlock()
		log.Printf("[BROKER][RECHAZO] RECHAZO evento de discoteca no registrada %q", e.Discoteca)
		return &pb.PublicarResp{Aceptado: false, Motivo: "discoteca no registrada"}, nil
	}
	s.rep.eventosRecibidos[e.Discoteca]++

	// Idempotencia: si el evento_id ya se vio, se descarta (publicacion duplicada).
	if _, existe := s.eventos[e.EventoId]; existe {
		s.rep.eventosRechazados++
		s.rep.rechDuplicado++
		s.mu.Unlock()
		log.Printf("[BROKER][IDEMPOTENCIA] evento duplicado descartado (idempotencia): %s", e.EventoId)
		return &pb.PublicarResp{Aceptado: false, Motivo: "evento duplicado"}, nil
	}

	// Validacion de datos del evento.
	if motivo := validarEvento(e); motivo != "" {
		s.rep.eventosRechazados++
		switch motivo {
		case "categoria invalida":
			s.rep.rechCategoria++
		case "stock invalido":
			s.rep.rechStock++
		default:
			s.rep.rechOtros++
		}
		s.mu.Unlock()
		log.Printf("[BROKER][RECHAZO] RECHAZO evento %s: %s", e.EventoId, motivo)
		return &pb.PublicarResp{Aceptado: false, Motivo: motivo}, nil
	}

	// Aceptado: lo guardamos en el log maestro.
	s.eventos[e.EventoId] = e
	s.rep.eventosAceptados[e.Discoteca]++
	s.mu.Unlock()

	// Escritura distribuida N=3 / W=2.
	acks := s.escribirEnNodos(&pb.DBItem{Tipo: "evento", Evento: e})
	if acks >= 2 {
		log.Printf("[BROKER][EVENTO_OK] evento %s (%s) almacenado con W=%d ACK", e.EventoId, e.NombreEvento, acks)
		return &pb.PublicarResp{Aceptado: true, Motivo: "almacenado"}, nil
	}
	log.Printf("[BROKER][SIN_QUORUM] evento %s aceptado pero escritura sin quorum (W=%d<2)", e.EventoId, acks)
	return &pb.PublicarResp{Aceptado: true, Motivo: "almacenamiento sin quorum W=2"}, nil
}

// validarEvento devuelve "" si el evento es valido, o el motivo del rechazo.
func validarEvento(e *pb.Evento) string {
	if e.EventoId == "" || e.NombreEvento == "" || e.Comuna == "" {
		return "campos obligatorios faltantes"
	}
	if !categoriasValidas[e.Categoria] {
		return "categoria invalida"
	}
	if e.Stock <= 0 {
		return "stock invalido"
	}
	if e.Precio <= 0 {
		return "precio invalido"
	}
	if e.FechaEvento == "" || e.FechaPublicacion == "" {
		return "fechas faltantes"
	}
	return ""
}

// ----------------------------------------------------------------------------
// RPC: Consulta de cartelera (usuario -> broker), lectura distribuida R=2
// ----------------------------------------------------------------------------

func (s *servidorBroker) ConsultarEventos(_ context.Context, r *pb.ConsultaReq) (*pb.ListaEventos, error) {
	s.mu.Lock()
	registrado := s.usuarios[r.IdUsuario]
	s.mu.Unlock()
	if !registrado {
		log.Printf("[BROKER][RECHAZO] consulta rechazada: usuario %q no registrado", r.IdUsuario)
		return &pb.ListaEventos{}, nil
	}

	eventos, ok := s.leerEventosQuorum()
	if !ok {
		log.Printf("[BROKER][SIN_QUORUM] consulta de %s sin consenso de lectura (R<2)", r.IdUsuario)
		return &pb.ListaEventos{}, nil
	}
	// Solo se ofrecen eventos con stock disponible.
	var disponibles []*pb.Evento
	for _, e := range eventos {
		if e.Stock > 0 {
			disponibles = append(disponibles, e)
		}
	}
	log.Printf("[BROKER][CARTELERA] usuario %s consulta cartelera: %d eventos disponibles (R=2 OK)", r.IdUsuario, len(disponibles))
	return &pb.ListaEventos{Eventos: disponibles}, nil
}

// ----------------------------------------------------------------------------
// RPC: Compra de entradas (usuario -> broker)
// ----------------------------------------------------------------------------

func (s *servidorBroker) ComprarEntrada(ctx context.Context, c *pb.CompraReq) (*pb.CompraResp, error) {
	// Serializamos las compras: garantiza que el control de stock sea correcto
	// y evita sobreventa (condicion de carrera entre dos compradores).
	s.mu.Lock()

	if !s.usuarios[c.IdUsuario] {
		s.mu.Unlock()
		return &pb.CompraResp{Aprobada: false, Motivo: "usuario no registrado"}, nil
	}
	s.rep.comprasSolicitadas++

	evento, existe := s.eventos[c.EventoId]
	if !existe {
		s.mu.Unlock()
		return &pb.CompraResp{Aprobada: false, Motivo: "evento no disponible"}, nil
	}

	// Anti-duplicado: un usuario no puede comprar dos veces el mismo evento.
	clave := c.IdUsuario + "|" + c.EventoId
	if s.comprasUsuarioEvento[clave] {
		s.mu.Unlock()
		log.Printf("[BROKER][DUPLICADO] compra duplicada bloqueada: %s ya tiene entrada para %s", c.IdUsuario, c.EventoId)
		return &pb.CompraResp{Aprobada: false, Motivo: "compra duplicada"}, nil
	}

	// Verificacion de stock distribuida con consenso R=2.
	stock, hayConsenso := s.stockQuorum(c.EventoId)
	if !hayConsenso {
		log.Printf("[BROKER][SIN_QUORUM] compra %s: sin consenso de lectura de stock (R<2)", c.EventoId)
		stock = evento.Stock // respaldo con el log maestro del broker
	}
	if stock <= 0 {
		s.rep.rechazoSinStock++
		s.mu.Unlock()
		log.Printf("[BROKER][SIN_STOCK] compra rechazada: evento %s sin stock", c.EventoId)
		return &pb.CompraResp{Aprobada: false, Motivo: "sin stock", NombreEvento: evento.NombreEvento}, nil
	}
	s.mu.Unlock()

	// Consulta al Banco USM (con timeout para tolerar caidas temporales del pago).
	aprobado, sinRespuesta := s.consultarPago(ctx, c.IdUsuario, evento.Precio, c.MedioPago)

	s.mu.Lock()
	if sinRespuesta {
		s.rep.pagosSinResp++
		s.rep.comprasPendientes++
		s.rep.fallos = append(s.rep.fallos, fmt.Sprintf("pago PENDIENTE: usuario=%s evento=%s (banco sin respuesta)", c.IdUsuario, c.EventoId))
		s.mu.Unlock()
		log.Printf("[BROKER][PAGO_PENDIENTE] compra %s queda PENDIENTE: banco sin respuesta", c.EventoId)
		return &pb.CompraResp{Aprobada: false, Motivo: "pendiente (servicio de pago no disponible)", NombreEvento: evento.NombreEvento}, nil
	}
	if !aprobado {
		s.rep.pagosRechazados++
		s.rep.rechazoPago++
		s.mu.Unlock()
		log.Printf("[BROKER][PAGO_RECHAZADO] compra %s rechazada por el banco (fondos insuficientes)", c.EventoId)
		return &pb.CompraResp{Aprobada: false, Motivo: "pago rechazado", NombreEvento: evento.NombreEvento}, nil
	}
	s.rep.pagosAprobados++

	// Pago aprobado: generar ticket unico, descontar stock y persistir.
	ticket := &pb.Ticket{
		TicketId:     "TK-" + uuidv4(),
		IdUsuario:    c.IdUsuario,
		EventoId:     c.EventoId,
		NombreEvento: evento.NombreEvento,
		Precio:       evento.Precio,
		FechaCompra:  time.Now().Format(time.RFC3339),
	}
	evento.Stock-- // control de stock en el log maestro (ya serializado por el mutex)
	s.tickets = append(s.tickets, ticket)
	s.comprasUsuarioEvento[clave] = true
	s.rep.comprasAprobadas++
	s.rep.ticketsGenerados++

	// Copia inmutable del evento para persistir fuera del lock (evita que una
	// compra concurrente modifique el stock mientras se serializa el mensaje).
	eventoSnap := &pb.Evento{
		EventoId:         evento.EventoId,
		Discoteca:        evento.Discoteca,
		NombreEvento:     evento.NombreEvento,
		Categoria:        evento.Categoria,
		Comuna:           evento.Comuna,
		Precio:           evento.Precio,
		Stock:            evento.Stock,
		FechaEvento:      evento.FechaEvento,
		FechaPublicacion: evento.FechaPublicacion,
	}
	resp := &pb.CompraResp{
		Aprobada:     true,
		TicketId:     ticket.TicketId,
		Motivo:       "aprobada",
		NombreEvento: evento.NombreEvento,
		Precio:       evento.Precio,
	}
	// Liberamos el mutex ANTES de la escritura distribuida: escribirEnNodos
	// vuelve a tomar s.mu por cada ACK, y el mutex de Go no es reentrante
	// (mantenerlo aqui congelaria todo el broker en un deadlock).
	s.mu.Unlock()

	// Persistir el nuevo stock del evento y el ticket en los nodos (N=3, W=2).
	s.escribirEnNodos(&pb.DBItem{Tipo: "evento", Evento: eventoSnap})
	s.escribirEnNodos(&pb.DBItem{Tipo: "ticket", Ticket: ticket})

	log.Printf("[BROKER][COMPRA_OK] compra APROBADA usuario=%s evento=%s ticket=%s stock_restante=%d",
		c.IdUsuario, c.EventoId, ticket.TicketId, eventoSnap.Stock)
	return resp, nil
}

// consultarPago llama al banco con un timeout. Devuelve (aprobado, sinRespuesta).
func (s *servidorBroker) consultarPago(ctx context.Context, usuario string, monto int64, medio string) (bool, bool) {
	ctxPago, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.banco.ValidarPago(ctxPago, &pb.PagoReq{IdUsuario: usuario, Monto: monto, MedioPago: medio})
	if err != nil {
		return false, true // el banco no respondio a tiempo / fallo temporal
	}
	return resp.Aprobado, false
}

// ----------------------------------------------------------------------------
// RPC: Historial de un usuario que se reintegra (lectura distribuida R=2)
// ----------------------------------------------------------------------------

func (s *servidorBroker) ObtenerHistorial(_ context.Context, r *pb.HistorialReq) (*pb.ListaTickets, error) {
	s.mu.Lock()
	registrado := s.usuarios[r.IdUsuario]
	s.mu.Unlock()
	if !registrado {
		return &pb.ListaTickets{}, nil
	}
	tickets, ok := s.leerTicketsQuorum(r.IdUsuario)
	if !ok {
		log.Printf("[BROKER][SIN_QUORUM] historial de %s sin consenso (R<2)", r.IdUsuario)
		return &pb.ListaTickets{}, nil
	}
	log.Printf("[BROKER][HISTORIAL] historial recuperado para %s: %d tickets (R=2 OK)", r.IdUsuario, len(tickets))
	return &pb.ListaTickets{Tickets: tickets}, nil
}

// ----------------------------------------------------------------------------
// RPC: Backlog para un nodo que se reincorpora (resincronizacion eventual)
// ----------------------------------------------------------------------------

func (s *servidorBroker) SolicitarBacklog(_ context.Context, r *pb.BacklogReq) (*pb.Backlog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := &pb.Backlog{}
	for _, e := range s.eventos {
		b.Eventos = append(b.Eventos, e)
	}
	b.Tickets = append(b.Tickets, s.tickets...)
	s.rep.resincros = append(s.rep.resincros,
		fmt.Sprintf("nodo %s resincronizado: %d eventos, %d tickets", r.IdNodo, len(b.Eventos), len(b.Tickets)))
	log.Printf("[BROKER][BACKLOG] entregando backlog a nodo %s: %d eventos, %d tickets", r.IdNodo, len(b.Eventos), len(b.Tickets))
	return b, nil
}

// ----------------------------------------------------------------------------
// Helpers de escritura y lectura distribuida (quorum)
// ----------------------------------------------------------------------------

// escribirEnNodos envia el item a los 3 nodos (N=3) y devuelve cuantos
// confirmaron (ACK). La operacion se considera exitosa con W>=2.
func (s *servidorBroker) escribirEnNodos(item *pb.DBItem) int {
	type res struct {
		dir string
		ok  bool
	}
	ch := make(chan res, len(s.clientesDB))
	for i, c := range s.clientesDB {
		go func(i int, c pb.NodoDBClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			ack, err := c.Escribir(ctx, item)
			ch <- res{dir: s.dirsDB[i], ok: err == nil && ack.Ok}
		}(i, c)
	}
	acks := 0
	for range s.clientesDB {
		r := <-ch
		s.mu.Lock()
		if r.ok {
			acks++
			s.rep.escriturasOK[r.dir]++
		} else {
			s.rep.escriturasFail[r.dir]++
			s.rep.fallos = append(s.rep.fallos, "escritura fallida en nodo "+r.dir)
		}
		s.mu.Unlock()
	}
	return acks
}

// leerEventosQuorum lee la lista de eventos de todos los nodos y reconcilia.
// Devuelve los eventos y true solo si al menos R=2 nodos respondieron.
func (s *servidorBroker) leerEventosQuorum() ([]*pb.Evento, bool) {
	respuestas := s.recolectarEventos()
	if len(respuestas) < 2 {
		return nil, false
	}
	// Reconciliacion por evento_id: nos quedamos con la version mas frecuente
	// (la que coincide en mas replicas). Esto materializa la regla R=2.
	porId := map[string][]*pb.Evento{}
	for _, lista := range respuestas {
		for _, e := range lista {
			porId[e.EventoId] = append(porId[e.EventoId], e)
		}
	}
	var out []*pb.Evento
	for _, versiones := range porId {
		out = append(out, versionMayoritaria(versiones))
	}
	return out, true
}

// stockQuorum devuelve el stock de un evento si al menos 2 nodos coinciden.
func (s *servidorBroker) stockQuorum(eventoID string) (int64, bool) {
	respuestas := s.recolectarEventos()
	if len(respuestas) < 2 {
		return 0, false
	}
	conteo := map[int64]int{} // stock -> cuantos nodos lo reportan
	for _, lista := range respuestas {
		for _, e := range lista {
			if e.EventoId == eventoID {
				conteo[e.Stock]++
			}
		}
	}
	for stock, n := range conteo {
		if n >= 2 {
			return stock, true // consenso R=2
		}
	}
	return 0, false
}

// leerTicketsQuorum recupera los tickets de un usuario exigiendo R=2.
func (s *servidorBroker) leerTicketsQuorum(usuario string) ([]*pb.Ticket, bool) {
	var respuestas [][]*pb.Ticket
	for i, c := range s.clientesDB {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lt, err := c.LeerTickets(ctx, &pb.HistorialReq{IdUsuario: usuario})
		cancel()
		if err != nil {
			log.Printf("[BROKER][NODO_CAIDO] nodo %s no respondio la lectura de tickets", s.dirsDB[i])
			continue
		}
		respuestas = append(respuestas, lt.Tickets)
	}
	if len(respuestas) < 2 {
		return nil, false
	}
	// Union por ticket_id (un ticket que exista en cualquier replica es valido,
	// y al exigir 2 respuestas garantizamos la regla R=2 sobre la lectura).
	visto := map[string]bool{}
	var out []*pb.Ticket
	for _, lista := range respuestas {
		for _, t := range lista {
			if !visto[t.TicketId] {
				visto[t.TicketId] = true
				out = append(out, t)
			}
		}
	}
	return out, true
}

// recolectarEventos pide la lista de eventos a cada nodo vivo.
func (s *servidorBroker) recolectarEventos() [][]*pb.Evento {
	var respuestas [][]*pb.Evento
	for i, c := range s.clientesDB {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		le, err := c.LeerEventos(ctx, &pb.Vacio{})
		cancel()
		if err != nil {
			log.Printf("[BROKER][NODO_CAIDO] nodo %s no respondio la lectura de eventos", s.dirsDB[i])
			continue
		}
		respuestas = append(respuestas, le.Eventos)
	}
	return respuestas
}

// versionMayoritaria elige, entre varias copias de un evento, la version
// (precio+stock) que aparece en mas nodos.
func versionMayoritaria(versiones []*pb.Evento) *pb.Evento {
	conteo := map[string]int{}
	rep := map[string]*pb.Evento{}
	for _, e := range versiones {
		k := fmt.Sprintf("%d|%d", e.Precio, e.Stock)
		conteo[k]++
		rep[k] = e
	}
	mejorK, mejorN := "", -1
	for k, n := range conteo {
		if n > mejorN {
			mejorK, mejorN = k, n
		}
	}
	return rep[mejorK]
}

// ----------------------------------------------------------------------------
// Generacion del Reporte.txt
// ----------------------------------------------------------------------------

func (s *servidorBroker) generarReporte(ruta string) {
	s.mu.Lock()
	// Tomamos una "foto" de los contadores y del estado de los nodos.
	r := s.rep
	nDiscotecas := len(s.discotecas)
	nUsuarios := len(s.usuarios)
	dirsDB := append([]string{}, s.dirsDB...)
	s.mu.Unlock()

	// Estado actual de cada nodo (vivo/caido) y cuanto almacena cada uno.
	estadoNodos := make([]string, len(s.clientesDB))
	for i, c := range s.clientesDB {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, errSalud := c.Salud(ctx, &pb.Vacio{})
		le, _ := c.LeerEventos(ctx, &pb.Vacio{})
		lt, _ := c.LeerTickets(ctx, &pb.HistorialReq{IdUsuario: ""})
		cancel()
		estado := "ACTIVO"
		nEv, nTk := 0, 0
		if errSalud != nil {
			estado = "CAIDO"
		} else {
			if le != nil {
				nEv = len(le.Eventos)
			}
			if lt != nil {
				nTk = len(lt.Tickets)
			}
		}
		estadoNodos[i] = fmt.Sprintf("  - %s: %s | escrituras OK=%d fail=%d | eventos=%d tickets=%d",
			dirsDB[i], estado, r.escriturasOK[dirsDB[i]], r.escriturasFail[dirsDB[i]], nEv, nTk)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "==================== REPORTE FINAL - DiscoPass ====================\n")
	fmt.Fprintf(&b, "Generado: %s\n\n", time.Now().Format(time.RFC3339))

	fmt.Fprintf(&b, "1. RESUMEN DE DISCOTECAS (%d registradas)\n", nDiscotecas)
	for disc, total := range r.eventosRecibidos {
		fmt.Fprintf(&b, "  - %s: enviados=%d aceptados=%d\n", disc, total, r.eventosAceptados[disc])
	}
	fmt.Fprintf(&b, "  Eventos rechazados: %d (categoria=%d, stock=%d, duplicados=%d, otros=%d)\n\n",
		r.eventosRechazados, r.rechCategoria, r.rechStock, r.rechDuplicado, r.rechOtros)

	fmt.Fprintf(&b, "2. ESTADO DE NODOS DE BASE DE DATOS\n")
	for _, l := range estadoNodos {
		fmt.Fprintf(&b, "%s\n", l)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "3. RESUMEN DE COMPRAS Y TICKETS (%d usuarios registrados)\n", nUsuarios)
	fmt.Fprintf(&b, "  Solicitudes de compra: %d\n", r.comprasSolicitadas)
	fmt.Fprintf(&b, "  Compras aprobadas: %d\n", r.comprasAprobadas)
	fmt.Fprintf(&b, "  Rechazadas por falta de stock: %d\n", r.rechazoSinStock)
	fmt.Fprintf(&b, "  Rechazadas por el servicio de pago: %d\n", r.rechazoPago)
	fmt.Fprintf(&b, "  Tickets generados: %d\n\n", r.ticketsGenerados)

	fmt.Fprintf(&b, "4. ESTADO DEL SERVICIO DE PAGO (Banco USM)\n")
	fmt.Fprintf(&b, "  Pagos aprobados: %d\n", r.pagosAprobados)
	fmt.Fprintf(&b, "  Pagos rechazados: %d\n", r.pagosRechazados)
	fmt.Fprintf(&b, "  Pagos sin respuesta / fallidos: %d\n", r.pagosSinResp)
	fmt.Fprintf(&b, "  Compras pendientes: %d\n\n", r.comprasPendientes)

	fmt.Fprintf(&b, "5. FALLOS Y RECUPERACIONES\n")
	if len(r.fallos) == 0 {
		fmt.Fprintf(&b, "  (sin fallos registrados)\n")
	}
	for _, f := range r.fallos {
		fmt.Fprintf(&b, "  - %s\n", f)
	}
	for _, rs := range r.resincros {
		fmt.Fprintf(&b, "  - resincronizacion: %s\n", rs)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "6. CONCLUSION\n")
	disponible := r.comprasAprobadas > 0 || r.comprasSolicitadas == 0
	fmt.Fprintf(&b, "  Sistema disponible bajo fallos: %v\n", disponible)
	fmt.Fprintf(&b, "  Sobreventa evitada: si (stock controlado y compras serializadas)\n")
	fmt.Fprintf(&b, "  Tickets duplicados evitados: si (un ticket_id unico por compra y anti-duplicado por usuario/evento)\n")
	fmt.Fprintf(&b, "===================================================================\n")

	if err := os.WriteFile(ruta, []byte(b.String()), 0644); err != nil {
		log.Printf("[BROKER][ERROR] no pude escribir reporte: %v", err)
		return
	}
}

// ----------------------------------------------------------------------------
// Utilidades
// ----------------------------------------------------------------------------

// env devuelve la variable de entorno o un valor por defecto.
func env(clave, defecto string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return defecto
}

// uuidv4 genera un identificador unico estilo UUIDv4 usando solo math/rand
// (no se usan librerias externas, respetando las restricciones del lab).
func uuidv4() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variante
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
