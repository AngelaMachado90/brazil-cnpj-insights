# ⚡ Energy Market Analytics Dashboard

[![Streamlit](https://img.shields.io/badge/Streamlit-1.23+-FF4B4B)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-✓-2496ED)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**Solução completa para análise de empresas no Mercado Livre de Energia**, integrando dados da Receita Federal e CCEE em um dashboard interativo.

👉 [Acesse a Demonstração](#) | 📽️ [Vídeo Tour](#)

---

## ✨ Destaques do Projeto

<div align="center">
  <img src="assets/screenshots/dash-main.gif" width="800" alt="Dashboard Interativo">
  <p><em>Visualização completa do fluxo de migração entre 2020-2025</em></p>
</div>

### 🎯 Principais Funcionalidades

| Feature | Descrição | Screenshot |
|---------|-----------|------------|
| **Análise Temporal** | Evolução das migrações por mês/ano | <img src="assets/screenshots/evolution.png" width="200"> |
| **Perfil das Empresas** | Detalhes cadastrais + societários | <img src="assets/screenshots/company-profile.png" width="200"> |
| **Filtros Inteligentes** | Busca por UF, CNAE e período | <img src="assets/screenshots/filters.png" width="200"> |
| **Exportação de Dados** | CSV completo com todos os campos | <img src="assets/screenshots/export.png" width="200"> |

---

## 🛠️ Tecnologias Utilizadas

<div align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="50" title="Python">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="50" title="PostgreSQL">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" width="50" title="Docker">
  <img src="https://streamlit.io/images/brand/streamlit-mark-color.svg" width="50" title="Streamlit">
</div>

**Stack Técnica:**
- **Backend**: Python + PostgreSQL (Dockerized)
- **ETL**: Pandas + Psycopg2
- **Visualização**: Streamlit + Plotly
- **Infra**: Docker Compose

---

## 📊 Screenshots Detalhadas

## 📸 Screenshots do Dashboard - Enriquecimento

### 1. Painel Estratégico
![Painel Estratégico](assets/screenshots/strategic-panel.jpg)

### 2. Evolução das Migrações
![Tendência de Migração](assets/screenshots/migration-trend.jpg)

### 3. Visão Detalhada
![Detalhes Empresariais](assets/screenshots/detailed-view.jpg)
---

## 🚀 Como Utilizar

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/energy-analytics.git

# Inicie os containers
docker-compose up -d