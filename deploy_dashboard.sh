#!/bin/bash

# Nome do container e imagem
CONTAINER_NAME="streamlit-app"
# Nome do serviço do dashboard (informativo apenas)
SERVICE_NAME="dashboard"
# Nome do Dockerfile (não é um arquivo docker-compose!)
DOCKERFILE="Dockerfile"
# Nome da rede Docker (já criada pelo Docker Compose)
NETWORK="projeto-cnpj_default"
# Porta do dashboard
PORT="8501"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Parando container antigo $CONTAINER_NAME..."
docker stop $CONTAINER_NAME 2>/dev/null

log "Removendo container antigo..."
docker rm $CONTAINER_NAME 2>/dev/null

log "Construindo imagem..."
docker build -t $CONTAINER_NAME -f $DOCKERFILE .

log "Subindo novo container..."
docker run -d --name $CONTAINER_NAME --network $NETWORK -p $PORT:$PORT $CONTAINER_NAME

log "Dashboard em execução!"
log "Acesse: http://localhost:$PORT"
log "Para ver os logs: docker logs -f $CONTAINER_NAME"
log "Para parar: docker stop $CONTAINER_NAME"
log "Para remover: docker rm $CONTAINER_NAME"
log "Para reiniciar: docker restart $CONTAINER_NAME"
