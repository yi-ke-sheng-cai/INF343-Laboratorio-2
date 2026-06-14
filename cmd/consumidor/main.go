package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"discopass/colores"
	pb "discopass/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	colores.Activar()
	id := flag.String("id", env("ID", "ClienteA"), "identificador del consumidor")
	dirBroker := flag.String("broker", env("BROKER", "localhost:50051"), "direccion del broker")
	medio := flag.String("medio", env("MEDIO_PAGO", "debito"), "medio de pago (credito/debito)")
	intervalo := flag.Int("intervalo", parseInt(env("INTERVALO", "10")), "segundos entre compras")
	csvRuta := flag.String("csv", env("CSV", ""), "ruta del archivo CSV (default: usuario_<id>.csv)")
	flag.Parse()

	if *csvRuta == "" {
		*csvRuta = "usuario_" + *id + ".csv"
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	connBroker, err := grpc.NewClient(*dirBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s][FATAL] no pude conectar al broker: %v", *id, err)
	}
	brokerPb := pb.NewBrokerClient(connBroker)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, err := brokerPb.RegistrarEntidad(ctx, &pb.Registro{Id: *id, Tipo: "usuario"})
		cancel()
		if err == nil && ack.Ok {
			log.Printf("[%s][REGISTRO] registrado en broker exitosamente", *id)
			break
		}
		log.Printf("[%s][ESPERA] esperando al broker... (%v)", *id, err)
		time.Sleep(2 * time.Second)
	}

	comprados := map[string]bool{}
	var mu sync.Mutex
	contadorCompras := 0

	for {
		mu.Lock()
		nCompras := contadorCompras
		mu.Unlock()

		if nCompras > 0 && nCompras%5 == 0 {
			log.Printf("[%s][DESCONEXION] simulando desconexion y recuperacion...", *id)
			mu.Lock()
			comprados = map[string]bool{}
			mu.Unlock()
			time.Sleep(3 * time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			historial, err := brokerPb.ObtenerHistorial(ctx, &pb.HistorialReq{IdUsuario: *id})
			cancel()
			if err != nil {
				log.Printf("[%s][ERROR] error recuperando historial: %v", *id, err)
			} else {
				mu.Lock()
				guardarCSV(*csvRuta, historial.Tickets, true)
				for _, t := range historial.Tickets {
					comprados[t.EventoId] = true
				}
				log.Printf("[%s][HISTORIAL] historial recuperado: %d tickets", *id, len(historial.Tickets))
				mu.Unlock()
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cartelera, err := brokerPb.ConsultarEventos(ctx, &pb.ConsultaReq{IdUsuario: *id})
		cancel()
		if err != nil {
			log.Printf("[%s][ERROR] error consultando eventos: %v", *id, err)
			time.Sleep(time.Duration(*intervalo) * time.Second)
			continue
		}

		mu.Lock()
		var disponibles []*pb.Evento
		for _, e := range cartelera.Eventos {
			if !comprados[e.EventoId] {
				disponibles = append(disponibles, e)
			}
		}
		mu.Unlock()

		if len(disponibles) == 0 {
			log.Printf("[%s][SIN_EVENTOS] no hay eventos disponibles sin comprar", *id)
			time.Sleep(time.Duration(*intervalo) * time.Second)
			continue
		}

		evento := disponibles[rng.Intn(len(disponibles))]
		log.Printf("[%s][COMPRANDO] comprando entrada para: %s (%s)", *id, evento.NombreEvento, evento.EventoId)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := brokerPb.ComprarEntrada(ctx2, &pb.CompraReq{
			IdUsuario: *id,
			EventoId:  evento.EventoId,
			MedioPago: *medio,
		})
		cancel2()
		if err != nil {
			log.Printf("[%s][ERROR] error en compra: %v", *id, err)
		} else if resp.Aprobada {
			log.Printf("[%s][COMPRA_OK] COMPRA APROBADA: ticket=%s evento=%s", *id, resp.TicketId, resp.NombreEvento)
			mu.Lock()
			comprados[evento.EventoId] = true
			contadorCompras++
			ticket := &pb.Ticket{
				TicketId:     resp.TicketId,
				IdUsuario:    *id,
				EventoId:     evento.EventoId,
				NombreEvento: resp.NombreEvento,
				Precio:       resp.Precio,
				FechaCompra:  time.Now().Format(time.RFC3339),
			}
			guardarCSV(*csvRuta, []*pb.Ticket{ticket}, false)
			mu.Unlock()
		} else {
			log.Printf("[%s][RECHAZO] compra RECHAZADA: motivo=%s", *id, resp.Motivo)
			if resp.Motivo == "sin stock" || resp.Motivo == "evento no disponible" || resp.Motivo == "compra duplicada" {
				mu.Lock()
				comprados[evento.EventoId] = true
				mu.Unlock()
			}
		}

		time.Sleep(time.Duration(*intervalo) * time.Second)
	}
}

func guardarCSV(ruta string, tickets []*pb.Ticket, sobreescribir bool) {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if sobreescribir {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	f, err := os.OpenFile(ruta, flags, 0644)
	if err != nil {
		log.Printf("[CSV][ERROR] error abriendo %s: %v", ruta, err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if sobreescribir {
		w.Write([]string{"ticket_id", "id_usuario", "evento_id", "nombre_evento", "precio", "fecha_compra"})
	}
	for _, t := range tickets {
		w.Write([]string{
			t.TicketId,
			t.IdUsuario,
			t.EventoId,
			t.NombreEvento,
			fmt.Sprintf("%d", t.Precio),
			t.FechaCompra,
		})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Printf("[CSV][ERROR] error escribiendo: %v", err)
	}
}

func env(clave, defecto string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return defecto
}

func parseInt(s string) int {
	var v int
	fmt.Sscanf(s, "%d", &v)
	return v
}
