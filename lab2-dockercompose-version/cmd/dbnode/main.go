package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"discopass/colores"
	pb "discopass/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type servidorNodoDB struct {
	pb.UnimplementedNodoDBServer

	mu       sync.Mutex
	id       string
	eventos  map[string]*pb.Evento
	tickets  map[string]*pb.Ticket
	caido    bool
	fallos   bool
	brokerPb pb.BrokerClient
}

func main() {
	colores.Activar()
	id := flag.String("id", env("ID", "DB1"), "identificador del nodo (DB1/DB2/DB3)")
	puerto := flag.String("puerto", env("PUERTO", "50061"), "puerto donde escucha")
	dirBroker := flag.String("broker", env("BROKER", "localhost:50051"), "direccion del broker")
	fallos := flag.Bool("fallos", envBool("FALLOS", true), "simular caidas y recuperaciones")
	flag.Parse()

	s := &servidorNodoDB{
		id:      *id,
		eventos: map[string]*pb.Evento{},
		tickets: map[string]*pb.Ticket{},
		fallos:  *fallos,
	}

	connBroker, err := grpc.NewClient(*dirBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s][FATAL] no pude conectar al broker: %v", *id, err)
	}
	s.brokerPb = pb.NewBrokerClient(connBroker)

	s.registrarEnBroker(*dirBroker)

	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[%s][FATAL] no pude escuchar en :%s: %v", *id, *puerto, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodoDBServer(grpcServer, s)

	if s.fallos {
		go s.simularFallos()
	}

	log.Printf("[%s][INICIO] escuchando en :%s | broker=%s | fallos=%v", *id, *puerto, *dirBroker, s.fallos)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s][FATAL] servidor detenido: %v", *id, err)
	}
}

func (s *servidorNodoDB) registrarEnBroker(dirBroker string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, err := s.brokerPb.RegistrarEntidad(ctx, &pb.Registro{Id: s.id, Tipo: "db"})
		cancel()
		if err == nil && ack.Ok {
			log.Printf("[%s][REGISTRO] registrado en broker exitosamente", s.id)
			return
		}
		log.Printf("[%s][ESPERA] esperando al broker para registrarse... (%v)", s.id, err)
		time.Sleep(2 * time.Second)
	}
}

func (s *servidorNodoDB) Escribir(_ context.Context, item *pb.DBItem) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caido {
		return &pb.Ack{Ok: false, Mensaje: "nodo caido"}, nil
	}
	switch item.Tipo {
	case "evento":
		if item.Evento != nil {
			s.eventos[item.Evento.EventoId] = item.Evento
			log.Printf("[%s][ESCRITURA_OK] escritura evento %s OK", s.id, item.Evento.EventoId)
		}
	case "ticket":
		if item.Ticket != nil {
			s.tickets[item.Ticket.TicketId] = item.Ticket
			log.Printf("[%s][ESCRITURA_OK] escritura ticket %s OK", s.id, item.Ticket.TicketId)
		}
	default:
		return &pb.Ack{Ok: false, Mensaje: "tipo desconocido"}, nil
	}
	return &pb.Ack{Ok: true, Mensaje: "almacenado"}, nil
}

func (s *servidorNodoDB) LeerEventos(_ context.Context, _ *pb.Vacio) (*pb.ListaEventos, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caido {
		return nil, fmt.Errorf("nodo %s caido", s.id)
	}
	lista := &pb.ListaEventos{}
	for _, e := range s.eventos {
		lista.Eventos = append(lista.Eventos, e)
	}
	return lista, nil
}

func (s *servidorNodoDB) LeerTickets(_ context.Context, r *pb.HistorialReq) (*pb.ListaTickets, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caido {
		return nil, fmt.Errorf("nodo %s caido", s.id)
	}
	lista := &pb.ListaTickets{}
	for _, t := range s.tickets {
		if r.IdUsuario == "" || t.IdUsuario == r.IdUsuario {
			lista.Tickets = append(lista.Tickets, t)
		}
	}
	return lista, nil
}

func (s *servidorNodoDB) Salud(_ context.Context, _ *pb.Vacio) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caido {
		return &pb.Ack{Ok: false, Mensaje: "nodo caido"}, nil
	}
	return &pb.Ack{Ok: true, Mensaje: "vivo"}, nil
}

func (s *servidorNodoDB) simularFallos() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(s.id[2])))
	for {
		segHastaCaer := 40 + rng.Intn(30)
		if s.id == "DB1" {
			segHastaCaer += 10
		} else if s.id == "DB2" {
			segHastaCaer += 25
		} else {
			segHastaCaer += 40
		}
		time.Sleep(time.Duration(segHastaCaer) * time.Second)

		s.mu.Lock()
		s.caido = true
		s.mu.Unlock()
		log.Printf("[%s][NODO_CAIDO] NODO CAIDO por %d segundos", s.id, segHastaCaer)

		duracionCaida := 15 + rng.Intn(11)
		time.Sleep(time.Duration(duracionCaida) * time.Second)

		s.mu.Lock()
		s.caido = false
		s.mu.Unlock()
		log.Printf("[%s][RECUPERACION] NODO RECUPERADO, solicitando backlog...", s.id)

		s.resincronizar()
	}
}

func (s *servidorNodoDB) resincronizar() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	backlog, err := s.brokerPb.SolicitarBacklog(ctx, &pb.BacklogReq{IdNodo: s.id})
	cancel()
	if err != nil {
		log.Printf("[%s][ERROR] error al solicitar backlog: %v", s.id, err)
		return
	}
	s.mu.Lock()
	s.eventos = map[string]*pb.Evento{}
	s.tickets = map[string]*pb.Ticket{}
	for _, e := range backlog.Eventos {
		s.eventos[e.EventoId] = e
	}
	for _, t := range backlog.Tickets {
		s.tickets[t.TicketId] = t
	}
	nEv := len(backlog.Eventos)
	nTk := len(backlog.Tickets)
	s.mu.Unlock()
	log.Printf("[%s][RESINCRONIZADO] resincronizado: %d eventos, %d tickets", s.id, nEv, nTk)
}

func env(clave, defecto string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return defecto
}

func envBool(clave string, defecto bool) bool {
	v := os.Getenv(clave)
	if v == "" {
		return defecto
	}
	return v == "true" || v == "1" || v == "yes"
}
