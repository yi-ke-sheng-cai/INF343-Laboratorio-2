package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"discopass/colores"
	pb "discopass/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type plantillaEvento struct {
	EventoId          string `json:"evento_id"`
	Discoteca         string `json:"discoteca"`
	NombreEvento      string `json:"nombre_evento"`
	Categoria         string `json:"categoria"`
	Comuna            string `json:"comuna"`
	Precio            int64  `json:"precio"`
	Stock             int64  `json:"stock"`
	FechaEvento       string `json:"fecha_evento"`
	FechaPublicacion  string `json:"fecha_publicacion"`
}

func main() {
	colores.Activar()
	discoteca := flag.String("discoteca", env("DISCOTECA", "DataClub"), "nombre de la discoteca")
	dirBroker := flag.String("broker", env("BROKER", "localhost:50051"), "direccion del broker")
	catalogo := flag.String("catalogo", env("CATALOGO", "config/catalogo_30.json"), "ruta del catalogo JSON")
	minSeg := flag.Int("min", parseInt(env("MIN_INTERVAL", "30")), "intervalo minimo entre publicaciones (seg)")
	maxSeg := flag.Int("max", parseInt(env("MAX_INTERVAL", "40")), "intervalo maximo entre publicaciones (seg)")
	flag.Parse()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	plantillas, err := leerCatalogo(*catalogo)
	if err != nil {
		log.Fatalf("[%s][FATAL] error leyendo catalogo: %v", *discoteca, err)
	}
	var misPlantillas []plantillaEvento
	for _, p := range plantillas {
		if p.Discoteca == *discoteca {
			misPlantillas = append(misPlantillas, p)
		}
	}
	if len(misPlantillas) == 0 {
		log.Fatalf("[%s][FATAL] no se encontraron eventos para esta discoteca en %s", *discoteca, *catalogo)
	}
	log.Printf("[%s][CATALOGO] encontradas %d plantillas", *discoteca, len(misPlantillas))

	connBroker, err := grpc.NewClient(*dirBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s][FATAL] no pude conectar al broker: %v", *discoteca, err)
	}
	brokerPb := pb.NewBrokerClient(connBroker)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, err := brokerPb.RegistrarEntidad(ctx, &pb.Registro{Id: *discoteca, Tipo: "discoteca"})
		cancel()
		if err == nil && ack.Ok {
			log.Printf("[%s][REGISTRO] registrado en broker exitosamente", *discoteca)
			break
		}
		log.Printf("[%s][ESPERA] esperando al broker... (%v)", *discoteca, err)
		time.Sleep(2 * time.Second)
	}

	idx := 0
	for {
		plantilla := misPlantillas[idx]
		idx = (idx + 1) % len(misPlantillas)

		evento := construirEvento(rng, &plantilla)

		reenvio := rng.Float64() < 0.20
		if reenvio {
			evento.EventoId = "RE-" + uuidv4()
			log.Printf("[%s][REENVIO] REENVIO (idempotencia) evento %s", *discoteca, evento.EventoId)
		}

		invalido := rng.Float64() < 0.08
		if invalido {
			catsInvalidas := []string{"Cumbia", "Jazz", "Metal", "Clasica", "Salsa"}
			evento.Categoria = catsInvalidas[rng.Intn(len(catsInvalidas))]
			log.Printf("[%s][INVALIDO_SIM] evento INVALIDO (categoria=%s) para probar validacion", *discoteca, evento.Categoria)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := brokerPb.PublicarEvento(ctx, evento)
		cancel()
		if err != nil {
			log.Printf("[%s][ERROR] error al publicar evento: %v", *discoteca, err)
		} else if resp.Aceptado {
			log.Printf("[%s][EVENTO_OK] evento ACEPTADO: %s", *discoteca, evento.NombreEvento)
		} else {
			log.Printf("[%s][RECHAZO] evento RECHAZADO: %s motivo=%s", *discoteca, evento.NombreEvento, resp.Motivo)
		}

		seg := *minSeg + rng.Intn(*maxSeg-*minSeg+1)
		time.Sleep(time.Duration(seg) * time.Second)
	}
}

func leerCatalogo(ruta string) ([]plantillaEvento, error) {
	data, err := os.ReadFile(ruta)
	if err != nil {
		return nil, err
	}
	var plantillas []plantillaEvento
	if err := json.Unmarshal(data, &plantillas); err != nil {
		return nil, err
	}
	return plantillas, nil
}

func construirEvento(rng *rand.Rand, p *plantillaEvento) *pb.Evento {
	precio := p.Precio + int64(float64(p.Precio)*0.2*(rng.Float64()*2-1))
	if precio < 1000 {
		precio = 1000
	}
	stock := int64(float64(p.Stock) * (0.5 + rng.Float64()*0.5))
	if stock < 1 {
		stock = 1
	}
	return &pb.Evento{
		EventoId:         uuidv4(),
		Discoteca:        p.Discoteca,
		NombreEvento:     p.NombreEvento,
		Categoria:        p.Categoria,
		Comuna:           p.Comuna,
		Precio:           precio,
		Stock:            stock,
		FechaEvento:      p.FechaEvento,
		FechaPublicacion: time.Now().Format(time.RFC3339),
	}
}

func uuidv4() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
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
