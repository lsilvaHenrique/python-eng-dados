from dotenv import load_dotenv
from pathlib import Path
import os
import requests
import json

# Carregar variáveis do .env
dotenv_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path)

WEATHER_KEY = os.getenv("WEATHER_KEY")
AIRVISUAL_KEY = os.getenv("AIRVISUAL_KEY")

def buscar_clima(cidade):
    """
    Busca informações climáticas de uma cidade utilizando a API WeatherAPI.
    Args:
        cidade (str): O nome da cidade para a qual as informações climáticas serão buscadas.
    Returns:
        dict: Um dicionário contendo os dados climáticos da cidade, caso a requisição seja bem-sucedida.
        None: Retorna None se ocorrer um erro durante a requisição.
    Exceptions:
    Exibe uma mensagem de erro no console caso a requisição falhe.
    """

    try:
        resposta = requests.get(f'http://api.weatherapi.com/v1/current.json?key={WEATHER_KEY}&q={cidade}')
        resposta.raise_for_status()  # Levanta um erro se a requisição falhar
        return resposta.json()  # Retorna os dados da resposta em formato JSON
    except Exception as e:
        print(f"Erro ao buscar clima: {e}")
        return None

def listar_paises_dados_qualidade_ar():
    """
    Lista os países disponíveis com dados de qualidade do ar utilizando a API AirVisual.
    Faz uma requisição HTTP GET para a API AirVisual para obter a lista de países
    que possuem dados de qualidade do ar. A chave de API (AIRVISUAL_KEY) deve estar
    definida no escopo do código.
    Returns:
        dict: Um dicionário contendo os dados da resposta em formato JSON, caso a
        requisição seja bem-sucedida.
    None: Retorna None se ocorrer algum erro durante a requisição.
    Raises:
    requests.exceptions.RequestException: Caso ocorra um erro relacionado à
    requisição HTTP.
    """
   
    try:
        resposta = requests.get(f"http://api.airvisual.com/v2/countries?key={AIRVISUAL_KEY}")
        resposta.raise_for_status()  # Levanta um erro se a requisição falhar
        return resposta.json()  # Retorna os dados da resposta em formato JSON
    except Exception as e:
        print(f"Erro ao buscar dados de qualidade do ar: {e}")
        return None
    

def listar_estado_dados_qualidade_ar(pais):
  
    try:
        resposta = requests.get(f"http://api.airvisual.com/v2/states?country={pais}&key={AIRVISUAL_KEY}")
        resposta.raise_for_status()  # Levanta um erro se a requisição falhar
        return resposta.json()  # Retorna os dados da resposta em formato JSON
    except Exception as e:
        print(f"Erro ao buscar dados de qualidade do ar: {e}")
        return None

def buscar_qualidade_ar(cidade, estado, pais):
    try:
        resposta = requests.get(f'http://api.airvisual.com/v2/city?city={cidade}&state={estado}&country={pais}&key={AIRVISUAL_KEY}')
        resposta.raise_for_status()  # Levanta um erro se a requisição falhar
        return resposta.json()  # Retorna os dados da resposta em formato JSON
    except Exception as e:
        print(f"Erro ao buscar qualidade do ar: {e}")
        return None

def lista_info_pelo_nome (pais):

    try:
        resposta = requests.get(f'https://restcountries.com/v3.1/name/{pais}')
        resposta.raise_for_status()  # Levanta um erro se a requisição falhar
        return resposta.json()  # Retorna os dados da resposta em formato JSON
    except Exception as e:
        print(f"Erro ao buscar dados de qualidade do ar: {e}")
        return None


def enriquecimento_dados_cidade(cidade):
    try:
        clima = buscar_clima(cidade)
        pais = clima["location"]["country"]
        estado = clima["location"]["region"]
        dadosPais = lista_info_pelo_nome(pais)
        dadosAr = buscar_qualidade_ar(cidade, estado, pais)
        dadosEnriquecidos = {
            "cidade": cidade,
            "clima": clima,
            "dadosPais": dadosPais,
            "dadosAr": dadosAr,
        }
        print(json.dumps(dadosEnriquecidos, indent=4, ensure_ascii=False))
    except Exception as e:
        print(f"Erro ao buscar dados de qualidade do ar: {e}")
        return None
    
#print(json.dumps(lista_info_pelo_nome("Spain"), indent=4, ensure_ascii=False))
enriquecimento_dados_cidade("São Paulo")
