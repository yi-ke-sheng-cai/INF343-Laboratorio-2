#!/usr/bin/env bash
# Banner de arranque de DiscoPass.
#
# Se imprime DESPUES de construir las imagenes Docker y ANTES de levantar los
# contenedores (antes de que empiecen los logs). Explica, de forma visual, el
# comportamiento esperado del sistema, como se conectan las partes y la leyenda
# de colores que usaran los logs a continuacion.
#
# Respeta NO_COLOR (https://no-color.org): si esta definido, no emite ANSI.

if [ -n "$NO_COLOR" ]; then
  B= D= R= RED= GRN= YEL= BLU= MAG= CYA=
else
  R=$'\033[0m'      # reset
  B=$'\033[1m'      # negrita
  D=$'\033[90m'     # gris (dim)
  RED=$'\033[31m'
  GRN=$'\033[32m'
  YEL=$'\033[33m'
  BLU=$'\033[34m'
  MAG=$'\033[35m'
  CYA=$'\033[36m'
fi

rule()  { printf '%s\n' "${D}════════════════════════════════════════════════════════════════════${R}"; }
say()   { printf '%b\n' "$1"; }

echo
rule
say "  ${B}${CYA}DiscoPass${R} ${B}· Sistema Distribuido de Venta de Entradas${R}"
say "  ${D}Go + gRPC/Protobuf · 5 tipos de proceso · replica N=3 (W=2 / R=2)${R}"
rule
echo

say "  ${B}TOPOLOGIA${R}   ${D}(el Broker es el UNICO que habla con DB y Banco)${R}"
echo
say "   ${YEL}${B}PRODUCTORES${R}                          ${MAG}${B}CONSUMIDORES${R}"
say "   ${YEL}discotecas${R}                            ${MAG}usuarios${R}"
say "   ${YEL}DataClub · DockersNight${R}               ${MAG}ClienteA · ClienteB${R}"
say "   ${YEL}GoLounge · GeorgieHouse${R}"
say "        ${D}│${R}                                     ${D}│${R}"
say "        ${D}│${R} ${D}PublicarEvento${R}                      ${D}│${R} ${D}ConsultarEventos${R}"
say "        ${D}│${R}                                     ${D}│${R} ${D}ComprarEntrada${R}"
say "        ${D}▼${R}                                     ${D}▼${R}"
say "     ${CYA}╭────────────────────────────────────────────╮${R}"
say "     ${CYA}│${R}                ${B}${CYA}BROKER${R}                      ${CYA}│${R}"
say "     ${CYA}│${R}   ${D}valida · quorum · stock · tickets${R}        ${CYA}│${R}"
say "     ${CYA}╰────────────────────────────────────────────╯${R}"
say "        ${D}│${R}                                     ${D}│${R}"
say "        ${D}│${R} ${D}Escribir/Leer  N=3 W=2 R=2${R}          ${D}│${R} ${D}ValidarPago${R}"
say "        ${D}▼${R}                                     ${D}▼${R}"
say "     ${BLU}${B}[DB1] [DB2] [DB3]${R}                     ${GRN}${B}BANCO USM${R}"
say "     ${BLU}replica con quorum${R}                    ${GRN}aprueba/rechaza pago${R}"
echo

say "  ${B}FLUJO ESPERADO${R}"
say "   ${CYA}1.${R} Productores publican eventos ${D}→${R} Broker valida ${D}(categoria, stock, idempotencia)${R}"
say "   ${CYA}2.${R} Broker replica en 3 nodos DB y confirma con ${B}W≥2${R} ${D}(quorum de escritura)${R}"
say "   ${CYA}3.${R} Consumidores consultan cartelera ${D}→${R} Broker lee con ${B}R≥2${R} ${D}(quorum de lectura)${R}"
say "   ${CYA}4.${R} Compra ${D}→${R} Broker verifica stock, consulta al Banco, genera ticket unico"
say "   ${CYA}5.${R} Si un nodo DB ${RED}cae${R}, al volver pide ${B}backlog${R} al Broker y se ${GRN}resincroniza${R}"
echo

say "  ${B}GARANTIAS${R}"
say "   ${GRN}✓${R} Sin sobreventa ${D}(stock serializado)${R}      ${GRN}✓${R} Sin tickets duplicados"
say "   ${GRN}✓${R} Tolerancia a caidas de DB y Banco     ${GRN}✓${R} Idempotencia por evento_id"
echo

say "  ${B}LEYENDA DE LOGS${R}   ${D}formato:${R}  ${D}hh:mm:ss${R} ${CYA}[Entidad]${R}${B}[EVENTO]${R} ${D}mensaje${R}"
say "   ${GRN}●${R} ${GRN}verde${R}  esperado / exito   ${D}REGISTRO · EVENTO_OK · COMPRA_OK · ESCRITURA_OK${R}"
say "   ${RED}●${R} ${RED}rojo${R}   error / inesperado  ${D}RECHAZO · SIN_QUORUM · SIN_STOCK · NODO_CAIDO${R}"
say "   ${D}●${R} ${D}gris${R}   neutro / ciclo vida  ${D}INICIO · ESPERA · COMPRANDO · IDEMPOTENCIA${R}"
echo
rule
say "  ${B}Levantando contenedores...${R} ${D}los logs comienzan a continuacion${R}"
rule
echo
