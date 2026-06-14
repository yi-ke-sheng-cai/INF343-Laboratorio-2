package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"discopass/colores"
	pb "discopass/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type servidorBanco struct {
	pb.UnimplementedBancoServer

	rng       *rand.Rand
	prob      float64
	probCred  float64
	fallos    bool
}

func main() {
	colores.Activar()
	puerto := flag.String("puerto", env("PUERTO", "50052"), "puerto del banco")
	dirBroker := flag.String("broker", env("BROKER", "localhost:50051"), "direccion del broker")
	prob := flag.Float64("prob", parseFloat(env("PROB", "0.80")), "probabilidad de aprobacion general")
	probCred := flag.Float64("prob-credito", parseFloat(env("PROB_CREDITO", "0.90")), "probabilidad de aprobacion para credito")
	fallos := flag.Bool("fallos", envBool("FALLOS", true), "simular fallos temporales")
	flag.Parse()

	s := &servidorBanco{
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		prob:     *prob,
		probCred: *probCred,
		fallos:   *fallos,
	}

	connBroker, err := grpc.NewClient(*dirBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[BANCO][FATAL] no pude conectar al broker: %v", err)
	}
	brokerPb := pb.NewBrokerClient(connBroker)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, err := brokerPb.RegistrarEntidad(ctx, &pb.Registro{Id: "BancoUSM", Tipo: "banco"})
		cancel()
		if err == nil && ack.Ok {
			log.Printf("[BANCO][REGISTRO] registrado en broker exitosamente")
			break
		}
		log.Printf("[BANCO][ESPERA] esperando al broker... (%v)", err)
		time.Sleep(2 * time.Second)
	}

	lis, err := net.Listen("tcp", ":"+*puerto)
	if err != nil {
		log.Fatalf("[BANCO][FATAL] no pude escuchar en :%s: %v", *puerto, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterBancoServer(grpcServer, s)
	log.Printf("[BANCO][INICIO] escuchando en :%s | broker=%s", *puerto, *dirBroker)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[BANCO][FATAL] servidor detenido: %v", err)
	}
}

func (s *servidorBanco) ValidarPago(_ context.Context, r *pb.PagoReq) (*pb.PagoResp, error) {
	if s.fallos && s.rng.Float64() < 0.05 {
		log.Printf("[BANCO][FALLO_SIM] fallo simulado - timeout para usuario=%s monto=%d", r.IdUsuario, r.Monto)
		time.Sleep(5 * time.Second)
		return nil, fmt.Errorf("banco caido temporalmente")
	}

	probActual := s.prob
	if r.MedioPago == "credito" {
		probActual = s.probCred
	}
	aprobado := s.rng.Float64() < probActual

	if aprobado {
		log.Printf("[BANCO][PAGO_OK] PAGO APROBADO: usuario=%s medio=%s monto=%d", r.IdUsuario, r.MedioPago, r.Monto)
	} else {
		log.Printf("[BANCO][PAGO_RECHAZADO] PAGO RECHAZADO: usuario=%s medio=%s monto=%d", r.IdUsuario, r.MedioPago, r.Monto)
	}
	return &pb.PagoResp{Aprobado: aprobado}, nil
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

func parseFloat(s string) float64 {
	var v float64
	fmt.Sscanf(s, "%f", &v)
	return v
}
