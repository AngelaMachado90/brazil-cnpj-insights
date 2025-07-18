FROM python:3.9-slim-buster as builder

# Instala todas as dependências de sistema necessárias
RUN apt-get update && \
    apt-get install -y \
    graphviz \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Primeiro copia apenas o requirements.txt para aproveitar o cache de camadas
COPY requirements.txt .

# Instala dependências Python (incluindo pandas)
RUN pip install --no-cache-dir -r requirements.txt

# --- Fase final (imagem reduzida) ---
FROM python:3.9-slim-buster

# Copia apenas o necessário da fase de construção
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Instala apenas as dependências de runtime
RUN apt-get update && \
    apt-get install -y \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia o código fonte
COPY . .

# Expor a porta do Streamlit
EXPOSE 8501

# Comando para rodar o app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]