"""
Módulo utilitário para extrair contatos estratégicos de uma página HTML e armazenar em banco de dados

Este módulo fornece funcionalidades robustas para extração de informações de contato de páginas web,
incluindo telefones, WhatsApp, e-mails, endereços físicos e links de redes sociais, com armazenamento em PostgreSQL.

Principais características:
- Download seguro de páginas HTML com tratamento de erros
- Extração abrangente de múltiplos tipos de contatos
- Armazenamento em banco de dados PostgreSQL
- Detecção avançada de padrões (regex + análise estrutural)
- Suporte a elementos modernos (como Elementor)
- Logging detalhado de todas as operações
- Saída padronizada em formato JSON

Desenvolvido por: [Seu Nome] | [Data] | Versão 2.0
"""

import os
import json
import requests
from bs4 import BeautifulSoup
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from logger_config import EmpresaLogger
from typing import Optional, Dict, List, Any

# Configuração inicial
empresa_logger = EmpresaLogger()
logger = empresa_logger.logger

# Padrões de regex pré-compilados para melhor performance
WHATSAPP_PATTERN = re.compile(r'WhatsApp:\s*([+\d\s\-\(\)]+)', re.IGNORECASE)
PHONE_PATTERN = re.compile(
    r'(?:tel|phone|telefone|fone)[\s:]*([+\d\s\-\(\)]{8,})|'  
    r'(\(?\d{2,3}\)?[\s\-]?\d{4,5}[\s\-]?\d{4})',
    re.IGNORECASE
)
EMAIL_PATTERN = re.compile(r'[\w\.-]+@[\w\.-]+')
ADDRESS_KEYWORDS = re.compile(r'RUA|rua|avenida|av\.|bairro|cep|\d{5}-\d{3}', re.I)

# Mapeamento de redes sociais
REDES_SOCIAIS = {
    'facebook': r'facebook\.com',
    'instagram': r'instagram\.com',
    'linkedin': r'linkedin\.com',
    'youtube': r'youtube\.com|youtu\.be'
}

class DatabaseManager:
    """Classe para gerenciar conexão e operações com o banco de dados PostgreSQL."""
    
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        """
        Inicializa o gerenciador de banco de dados.
        
        Args:
            dbname: Nome do banco de dados
            user: Nome de usuário
            password: Senha do banco de dados
            host: Endereço do servidor (padrão: localhost)
            port: Porta do PostgreSQL (padrão: 5432)
        """
        self.conn_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.connection = None
        
    def connect(self) -> bool:
        """Estabelece conexão com o banco de dados."""
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            logger.info("Conexão com o banco de dados estabelecida com sucesso", extra={'empresa': 'SISTEMA'})
            return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {str(e)}", extra={'empresa': 'SISTEMA'})
            return False
            
    def disconnect(self):
        """Fecha a conexão com o banco de dados."""
        if self.connection:
            self.connection.close()
            logger.info("Conexão com o banco de dados encerrada", extra={'empresa': 'SISTEMA'})
    
    def save_contacts(self, cnpj: str, razao_social: str, contatos: Dict[str, Any]) -> bool:
        """
        Salva ou atualiza os contatos no banco de dados.
        
        Args:
            cnpj: CNPJ da empresa (14 dígitos)
            razao_social: Razão social da empresa
            contatos: Dicionário com os contatos extraídos
            
        Returns:
            True se a operação foi bem sucedida, False caso contrário
        """
        if not self.connection:
            logger.error("Tentativa de salvar contatos sem conexão com o banco", extra={'empresa': 'SISTEMA'})
            return False
            
        try:
            with self.connection.cursor() as cursor:
                # Verifica se o CNPJ já existe
                query = sql.SQL("""
                    INSERT INTO dados_enriquecidos (cnpj, razao_social, telefones, whatsapp, emails, endereco, redes_sociais)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cnpj) 
                    DO UPDATE SET 
                        razao_social = EXCLUDED.razao_social,
                        telefones = EXCLUDED.telefones,
                        whatsapp = EXCLUDED.whatsapp,
                        emails = EXCLUDED.emails,
                        endereco = EXCLUDED.endereco,
                        redes_sociais = EXCLUDED.redes_sociais,
                        data_atualizacao = CURRENT_TIMESTAMP
                    RETURNING id;
                """)
                
                cursor.execute(query, (
                    cnpj,
                    razao_social,
                    Json(contatos['telefones']),
                    contatos['whatsapp'],
                    Json(contatos['emails']),
                    contatos['endereco'],
                    Json(contatos['redes_sociais'])
                ))
                
                record_id = cursor.fetchone()[0]
                self.connection.commit()
                logger.info(f"Contatos salvos/atualizados no banco para CNPJ: {cnpj} (ID: {record_id})", 
                          extra={'empresa': 'SISTEMA'})
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Erro ao salvar contatos no banco de dados: {str(e)}", extra={'empresa': 'SISTEMA'})
            self.connection.rollback()
            return False

def baixar_html(url: str, headers: dict = None, timeout: int = 15) -> str:
    """
    Baixa o conteúdo HTML de uma URL com tratamento robusto de erros.
    
    Args:
        url: Endereço web para download
        headers: Cabeçalhos HTTP personalizados (opcional)
        timeout: Tempo máximo de espera em segundos (padrão: 15)
    
    Returns:
        String com conteúdo HTML ou None em caso de falha
    
    Exemplo:
        >>> html = baixar_html("https://exemplo.com")
        >>> if html:
        ...     print(f"HTML baixado com {len(html)} caracteres")
    """
    try:
        logger.info(f"Iniciando download de: {url}", extra={'empresa': 'SISTEMA'})
        resp = requests.get(
            url, 
            headers=headers, 
            timeout=timeout, 
            verify=False
        )
        resp.raise_for_status()
        logger.info(f"Download concluído: {url} (status {resp.status_code})", 
                  extra={'empresa': 'SISTEMA'})
        return resp.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha no download de {url}: {str(e)}", 
                   extra={'empresa': 'SISTEMA'})
        return None

def extrair_contatos_estrategicos(
    soup: BeautifulSoup,
    url: str,
    nome_empresa: str = None,
    redes_sociais: dict = None
) -> dict:
    """
    Extrai todos os contatos estratégicos de uma página HTML parseada.
    
    Args:
        soup: Objeto BeautifulSoup com o conteúdo parseado
        url: URL de origem para logs
        nome_empresa: Nome da empresa para contexto (opcional)
        redes_sociais: Dicionário pré-existente de redes sociais (opcional)
    
    Returns:
        Dicionário estruturado com:
        {
            'telefones': [lista de números],
            'whatsapp': link do WhatsApp ou None,
            'emails': [lista de e-mails],
            'endereco': texto do endereço ou None,
            'redes_sociais': {
                'facebook': url,
                'instagram': url,
                'linkedin': url,
                'youtube': url
            }
        }
    
    Exemplo:
        >>> soup = BeautifulSoup(html, 'html.parser')
        >>> contatos = extrair_contatos_estrategicos(soup, "https://exemplo.com")
    """
    empresa_log = nome_empresa if nome_empresa else 'SISTEMA'
    logger.info(f"Iniciando extração de contatos de: {url}", 
              extra={'empresa': empresa_log})

    contatos = {
        'telefones': [],
        'whatsapp': None,
        'emails': [],
        'endereco': None,
        'redes_sociais': redes_sociais if redes_sociais else {
            'linkedin': None,
            'facebook': None,
            'instagram': None,
            'youtube': None
        }
    }

    # Extração de WhatsApp
    _extrair_whatsapp(soup, contatos, empresa_log)
    
    # Extração de telefones
    _extrair_telefones(soup, contatos, empresa_log)
    
    # Extração de e-mails
    _extrair_emails(soup, contatos, empresa_log)
    
    # Extração de redes sociais
    _extrair_redes_sociais(soup, contatos, empresa_log)
    
    # Extração de endereços
    _extrair_enderecos(soup, contatos, empresa_log)

    logger.info(f"Extracção concluída para {empresa_log}", 
              extra={'empresa': empresa_log})
    return contatos

def salvar_contatos(contatos: dict, filepath: str = 'data/contatos_empresas.json') -> None:
    """
    Salva os contatos extraídos em arquivo JSON com estrutura padronizada.
    
    Args:
        contatos: Dicionário de contatos a ser salvo
        filepath: Caminho do arquivo de destino (padrão: data/contatos_empresas.json)
    
    Raises:
        IOError: Se não for possível salvar o arquivo
    
    Exemplo:
        >>> contatos = {...}  # Dicionário de contatos
        >>> salvar_contatos(contatos, 'dados/contatos.json')
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(contatos, f, ensure_ascii=False, indent=4)
        logger.info(f"Contatos salvos em {filepath}", extra={'empresa': 'SISTEMA'})
    except Exception as e:
        logger.error(f"Erro ao salvar contatos: {str(e)}", extra={'empresa': 'SISTEMA'})
        raise

# ========== FUNÇÕES PRIVADAS ==========

def _extrair_whatsapp(soup: BeautifulSoup, contatos: dict, empresa_log: str) -> None:
    """Extrai número de WhatsApp do texto da página."""
    whatsapp_match = WHATSAPP_PATTERN.search(soup.get_text())
    if whatsapp_match:
        whatsapp_num = re.sub(r'[^\d+]', '', whatsapp_match.group(1))
        contatos['whatsapp'] = f'https://api.whatsapp.com/send?phone={whatsapp_num}'
        contatos['telefones'].append(whatsapp_num)
        logger.info(f"WhatsApp detectado: {whatsapp_num}", extra={'empresa': empresa_log})

def _extrair_telefones(soup: BeautifulSoup, contatos: dict, empresa_log: str) -> None:
    """Extrai todos os números de telefone da página."""
    # Busca em textos
    text_elements = soup.find_all(['p', 'div', 'span', 'li'])
    for element in text_elements:
        text = element.get_text(" ", strip=True)
        matches = PHONE_PATTERN.findall(text)
        for match in matches:
            for m in match:
                if m:
                    phone_num = re.sub(r'[^\d+]', '', m)
                    if phone_num and phone_num not in contatos['telefones']:
                        contatos['telefones'].append(phone_num)
                        logger.debug(f"Telefone encontrado: {phone_num}", 
                                   extra={'empresa': empresa_log})

    # Busca em links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('tel:'):
            phone_num = re.sub(r'[^\d+]', '', href.replace('tel:', ''))
            if phone_num and phone_num not in contatos['telefones']:
                contatos['telefones'].append(phone_num)
                logger.info(f"Telefone em link detectado: {phone_num}", 
                          extra={'empresa': empresa_log})

def _extrair_emails(soup: BeautifulSoup, contatos: dict, empresa_log: str) -> None:
    """Extrai endereços de e-mail da página."""
    # Busca em textos
    email_elements = soup.find_all(['a', 'p', 'div', 'span'])
    for element in email_elements:
        text = element.get_text(" ", strip=True)
        matches = EMAIL_PATTERN.findall(text)
        for email in matches:
            if email not in contatos['emails']:
                contatos['emails'].append(email)
                logger.debug(f"E-mail encontrado: {email}", extra={'empresa': empresa_log})

    # Busca em links mailto:
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('mailto:'):
            email = href.replace('mailto:', '').strip()
            if email and email not in contatos['emails']:
                contatos['emails'].append(email)
                logger.info(f"E-mail em link detectado: {email}", 
                          extra={'empresa': empresa_log})

def _extrair_redes_sociais(soup: BeautifulSoup, contatos: dict, empresa_log: str) -> None:
    """Extrai links de redes sociais da página."""
    for link in soup.find_all('a', href=True):
        href = link['href'].lower()
        for rede, padrao in REDES_SOCIAIS.items():
            if re.search(padrao, href, re.I):
                if not contatos['redes_sociais'][rede]:
                    contatos['redes_sociais'][rede] = href
                    logger.info(f"Rede social detectada: {rede} -> {href}", 
                              extra={'empresa': empresa_log})

def _extrair_enderecos(soup: BeautifulSoup, contatos: dict, empresa_log: str) -> None:
    """Extrai endereços físicos da página com múltiplas estratégias."""
    enderecos = []

    # Estratégia 1: Busca por elementos com padrão de endereço
    address_elements = soup.find_all(['p', 'div', 'li', 'ul'], 
                                   string=ADDRESS_KEYWORDS)
    if not address_elements:
        address_elements = soup.find_all(['p', 'div', 'li'], 
                                       class_=re.compile(r'address|endereco|local', re.I))
    
    for element in address_elements:
        text = element.get_text(" ", strip=True)
        if _validar_endereco(text):
            enderecos.append(text)
            logger.debug(f"Endereço potencial: {text[:50]}...", 
                       extra={'empresa': empresa_log})

    # Estratégia 2: Busca por spans do Elementor
    for span in soup.find_all('span', class_=re.compile(r'elementor-icon-list-text', re.I)):
        text = span.get_text(" ", strip=True)
        if _validar_endereco(text) and text not in enderecos:
            enderecos.append(text)
            logger.info(f"Endereço em Elementor: {text[:50]}...", 
                       extra={'empresa': empresa_log})

    contatos['endereco'] = enderecos[0] if enderecos else None

def _validar_endereco(texto: str) -> bool:
    """Valida se um texto parece ser um endereço físico."""
    return (
        re.search(r'\d', texto) and
        any(term in texto.lower() for term in ['rua', 'av', 'avenida', 'bairro', 'cep']) and
        len(texto.split()) >= 5
    )

if __name__ == "__main__":
    logger.info(
        "Módulo de extração de contatos - Modo utilitário",
        extra={'empresa': 'SISTEMA'}
    )
    print("Este módulo deve ser importado por outros scripts.")