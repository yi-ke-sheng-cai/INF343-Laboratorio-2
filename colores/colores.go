// Paquete colores: colorea la salida estandar de log sin tocar cada Printf.
//
// Funcionamiento: Activar() reemplaza el destino del logger por un io.Writer
// que intercepta cada linea ya formateada
// ("YYYY/MM/DD hh:mm:ss [Entidad][EVENTO] msg") y le aplica codigos ANSI:
//   - 1a etiqueta [Entidad]: color estable por nombre (misma entidad => mismo
//     color siempre).
//   - 2a etiqueta [EVENTO]: color por clasificacion del evento:
//       * verde  -> evento esperado (exito del flujo normal).
//       * rojo   -> evento inesperado / error / fallo.
//       * gris   -> evento neutro (ciclo de vida, info, simulaciones).
//   - En el cuerpo del mensaje: palabras de exito en verde y de fallo en rojo.
//
// El logger estandar emite cada registro con un unico Write, asi que procesar
// por Write equivale a procesar por linea.
package colores

import (
	"hash/fnv"
	"log"
	"os"
	"regexp"
)

const (
	reset = "\x1b[0m"
	verde = "\x1b[32m"
	rojo  = "\x1b[31m"
	gris  = "\x1b[90m"
)

// paleta para las etiquetas [Entidad]. Se excluyen verde/rojo/gris porque
// quedan reservados para la clasificacion de eventos y los estados OK/CANCEL.
var paleta = []string{
	"\x1b[36m", // cyan
	"\x1b[35m", // magenta
	"\x1b[33m", // amarillo
	"\x1b[34m", // azul
	"\x1b[96m", // cyan claro
	"\x1b[95m", // magenta claro
	"\x1b[94m", // azul claro
	"\x1b[93m", // amarillo claro
}

// Clasificacion de los nombres de [EVENTO]. Lo que no este aqui es neutro (gris).
var (
	eventosVerde = map[string]bool{
		"REGISTRO": true, "EVENTO_OK": true, "COMPRA_OK": true, "PAGO_OK": true,
		"ESCRITURA_OK": true, "CARTELERA": true, "HISTORIAL": true, "BACKLOG": true,
		"RESINCRONIZADO": true, "RECUPERACION": true,
	}
	eventosRojo = map[string]bool{
		"FATAL": true, "ERROR": true, "RECHAZO": true, "SIN_QUORUM": true,
		"SIN_STOCK": true, "PAGO_RECHAZADO": true, "PAGO_PENDIENTE": true,
		"NODO_CAIDO": true, "FALLO_SIM": true,
	}
)

var (
	reEtiqueta = regexp.MustCompile(`\[[^\]]+\]`)
	rePos      = regexp.MustCompile(`\b(OK|APROBAD[OA]|ACEPTADO|aceptado|almacenado|exitosamente|RECUPERADO|recuperado|resincronizado|disponibles?|vivo)\b`)
	reNeg      = regexp.MustCompile(`\b(CANCEL|DeadlineExceeded|error|RECHAZO|RECHAZAD[OA]|rechazad[oa]|CAIDO|caido|INVALIDO|fallid[oa]s?|fallo|PENDIENTE|duplicad\w*|no respondio|no registrada|sin quorum|sin consenso|sin stock)\b`)
)

// colorEtiqueta asigna un color estable a cada nombre de entidad.
func colorEtiqueta(nombre string) string {
	h := fnv.New32a()
	h.Write([]byte(nombre))
	return paleta[h.Sum32()%uint32(len(paleta))]
}

// colorEvento clasifica el nombre del evento: verde/rojo/gris.
func colorEvento(nombre string) string {
	switch {
	case eventosVerde[nombre]:
		return verde
	case eventosRojo[nombre]:
		return rojo
	default:
		return gris // neutro
	}
}

type escritor struct{ destino *os.File }

func (e escritor) Write(p []byte) (int, error) {
	s := string(p)

	// 1a etiqueta = [Entidad], la que sigue a la hora.
	if loc := reEtiqueta.FindStringIndex(s); loc != nil {
		etq := s[loc[0]:loc[1]]
		nombre := etq[1 : len(etq)-1]
		prefijo := s[:loc[0]]
		cuerpo := colorEtiqueta(nombre) + etq + reset
		resto := s[loc[1]:]

		// 2a etiqueta = [EVENTO], pegada justo despues de la entidad.
		if loc2 := reEtiqueta.FindStringIndex(resto); loc2 != nil && loc2[0] == 0 {
			etq2 := resto[:loc2[1]]
			evento := etq2[1 : len(etq2)-1]
			cuerpo += colorEvento(evento) + etq2 + reset
			resto = resto[loc2[1]:]
		}

		// Palabras de estado solo en el cuerpo del mensaje (no en las etiquetas).
		resto = rePos.ReplaceAllString(resto, verde+"$1"+reset)
		resto = reNeg.ReplaceAllString(resto, rojo+"$1"+reset)

		s = prefijo + cuerpo + resto
	}

	if _, err := e.destino.Write([]byte(s)); err != nil {
		return 0, err
	}
	return len(p), nil // io.Writer: devolver el largo original consumido
}

// Activar redirige el log estandar al escritor con colores. Llamar una vez al
// inicio de cada main, antes de los primeros log.Printf.
func Activar() {
	log.SetOutput(escritor{destino: os.Stderr})
}
