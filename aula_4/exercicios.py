import os
import time
import requests
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# String de conexão
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
db = os.getenv("PG_DB")
weather_key = os.getenv("WEATHER_KEY")
airvisual_key = os.getenv("AIRVISUAL_KEY")

engine = psycopg2.connect(f"postgresql://{user}:{password}@{host}/{db}?sslmode=require")


def run_query(sql, coon=engine):
    return pd.read_sql_query(sql, coon)

# Exercício 1
def exercicio1_temperatura_media(run_query, get_temperatura):
    query = '''
    SELECT ci.city, COUNT(p.payment_id) as num_transacoes, COUNT(DISTINCT c.customer_id) as num_clientes
    FROM payment p
    JOIN customer c ON p.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    GROUP BY ci.city
    HAVING COUNT(p.payment_id) > 10
    '''
    df_cidades = run_query(query)
    df_cidades["temperatura"] = df_cidades["city"].apply(get_temperatura)
    df_cidades.dropna(subset=["temperatura"], inplace=True)
    total_clientes = df_cidades["num_clientes"].sum()
    media_ponderada = (df_cidades["temperatura"] * df_cidades["num_clientes"]).sum() / total_clientes
    print(f"Temperatura média ponderada: {media_ponderada:.2f}°C")
    print(df_cidades.sort_values(by="temperatura", ascending=False))

# Exercício 2
def get_temperatura(cidade):
    try:
        r = requests.get(f"http://api.weatherapi.com/v1/current.json?key={weather_key}&q={cidade}", timeout=5)
        return r.json()["current"]["temp_c"]
    except:
        return None

def exercicio2_receita_amena(run_query, get_temperatura):
    query = '''
    SELECT ci.city, SUM(p.amount) as receita_total
    FROM payment p
    JOIN customer c ON p.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    GROUP BY ci.city
    ORDER BY receita_total DESC
    '''
    df = run_query(query)
    df["temperatura"] = df["city"].apply(get_temperatura)
    df.dropna(subset=["temperatura"], inplace=True)
    df_ameno = df[(df["temperatura"] >= 18) & (df["temperatura"] <= 24)]
    total = df_ameno["receita_total"].sum()
    print(f"Receita total em cidades entre 18°C e 24°C: ${total:.2f}")
    print(df_ameno.sort_values(by="receita_total", ascending=False))

# Exercício 3
def get_populacao(pais):
    try:
        r = requests.get(f"https://restcountries.com/v3.1/name/{pais}", timeout=5)
        return r.json()[0]["population"]
    except:
        return None

def exercicio3_alugueis_por_populacao(run_query, get_populacao):
    query = '''
    SELECT co.country, COUNT(r.rental_id) as num_alugueis
    FROM rental r
    JOIN customer c ON r.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    GROUP BY co.country
    ORDER BY num_alugueis DESC
    '''
    df = run_query(query)
    df["populacao"] = df["country"].apply(get_populacao)
    df.dropna(subset=["populacao"], inplace=True)
    df["alugueis_por_1000"] = (df["num_alugueis"] / df["populacao"]) * 1000
    print(df.sort_values(by="alugueis_por_1000", ascending=False))

# Exercício 4
def get_aqi(cidade):
    try:
        r = requests.get(f"http://api.airvisual.com/v2/city?city={cidade}&key={airvisual_key}", timeout=5)
        return r.json()["data"]["current"]["pollution"]["aqius"]
    except:
        return None

def exercicio4_filmes_poluidos(run_query, get_aqi):
    query_cidades = '''
    SELECT ci.city, COUNT(c.customer_id) as num_clientes
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    GROUP BY ci.city
    ORDER BY num_clientes DESC
    LIMIT 10
    '''
    df_cidades = run_query(query_cidades)
    df_cidades["AQI"] = df_cidades["city"].apply(get_aqi)
    poluidas = df_cidades[df_cidades["AQI"] > 150]["city"].tolist()
    if poluidas:
        cidades_str = ",".join([f"'{c}'" for c in poluidas])
        query_filmes = f'''
        SELECT f.title, ci.city, COUNT(r.rental_id) as alugueis
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN customer c ON r.customer_id = c.customer_id
        JOIN address a ON c.address_id = a.address_id
        JOIN city ci ON a.city_id = ci.city_id
        WHERE ci.city IN ({cidades_str})
        GROUP BY f.title, ci.city
        ORDER BY alugueis DESC
        '''
        df_filmes = run_query(query_filmes)
        print(df_filmes)

# Exercício 5
def exercicio5_clientes_areas_criticas(run_query, get_aqi, get_temperatura):
    query = '''
    SELECT c.first_name, c.last_name, ci.city, co.country
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    '''
    df = run_query(query)
    df["AQI"] = df["city"].apply(get_aqi)
    df["temperatura"] = df["city"].apply(get_temperatura)
    df = df[(df["AQI"] > 130) & (df["temperatura"].notnull())]
    df["zona_atencao"] = "Sim"
    print(df)

# Exercício 6
def exercicio6_receita_por_continente(run_query, get_populacao):
    query = '''
    SELECT co.country, SUM(p.amount) as receita_total
    FROM payment p
    JOIN customer c ON p.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    GROUP BY co.country
    '''
    df = run_query(query)
    df["continente"] = df["country"].apply(lambda x: requests.get(f"https://restcountries.com/v3.1/name/{x}").json()[0]["region"] if requests.get(f"https://restcountries.com/v3.1/name/{x}").ok else None)
    df.dropna(subset=["continente"], inplace=True)
    receita_por_continente = df.groupby("continente")["receita_total"].sum()
    receita_por_continente.plot.pie(autopct='%1.1f%%')
    plt.title("Receita por Continente")
    plt.show()

# Exercício 7
def exercicio7_tempo_medio(run_query, get_temperatura):
    query = '''
    SELECT ci.city, AVG(EXTRACT(epoch FROM (r.return_date - r.rental_date))/3600) as tempo_medio_horas
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN store s ON i.store_id = s.store_id
    JOIN address a ON s.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    GROUP BY ci.city
    '''
    df = run_query(query)
    df["temperatura"] = df["city"].apply(get_temperatura)
    df.dropna(subset=["temperatura"], inplace=True)
    sns.scatterplot(x="temperatura", y="tempo_medio_horas", data=df)
    sns.regplot(x="temperatura", y="tempo_medio_horas", data=df, scatter=False, color="red")
    plt.title("Tempo Médio de Aluguel vs Temperatura")
    plt.xlabel("Temperatura (°C)")
    plt.ylabel("Tempo Médio de Aluguel (horas)")
    plt.show()

# Exercício 8
def exercicio8_perfil_clima(run_query, get_aqi, get_temperatura):
    query = '''
    SELECT c.customer_id, c.first_name, c.last_name, ci.city, COUNT(r.rental_id) as total_alugueis, SUM(p.amount) as gasto_total
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN rental r ON c.customer_id = r.customer_id
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY c.customer_id, ci.city
    '''
    df = run_query(query)
    df["AQI"] = df["city"].apply(get_aqi)
    df["temperatura"] = df["city"].apply(get_temperatura)
    df.dropna(subset=["AQI", "temperatura"], inplace=True)
    df["faixa_etaria"] = pd.cut(df.index, bins=3, labels=["Jovem", "Adulto", "Sênior"])
    print(df.groupby("faixa_etaria").mean())

# Exercício 9
def exercicio9_exportar_excel(run_query, get_aqi, get_temperatura):
    query = '''
    SELECT c.customer_id, c.first_name, c.last_name, ci.city, co.country, SUM(p.amount) as receita
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    JOIN payment p ON c.customer_id = p.customer_id
    GROUP BY c.customer_id, ci.city, co.country
    '''
    df = run_query(query)
    df["AQI"] = df["city"].apply(get_aqi)
    df["temperatura"] = df["city"].apply(get_temperatura)
    media_receita = df["receita"].mean()
    filtro = (df["temperatura"] < 15) & (df["AQI"] > 100) & (df["receita"] > media_receita)
    df_filtrado = df[filtro]
    with pd.ExcelWriter("relatorio_clientes.xlsx") as writer:
        df_filtrado.to_excel(writer, sheet_name="Clientes", index=False)
        df[["city", "temperatura"]].to_excel(writer, sheet_name="Temperaturas", index=False)
        df[["city", "AQI"]].to_excel(writer, sheet_name="Alertas", index=False)

# Exercício 10
def exercicio10_cache_clima(get_temperatura, cidade, cache={}):
    if cidade in cache:
        return cache[cidade]
    else:
        temp = get_temperatura(cidade)
        cache[cidade] = temp
        return temp

# Exemplo de uso
#exercicio1_temperatura_media(run_query, get_temperatura)
# exercicio2_receita_amena(run_query, get_temperatura)
# exercicio3_alugueis_por_populacao(run_query, get_populacao)
# exercicio4_filmes_poluidos(run_query, get_aqi)
# exercicio5_clientes_areas_criticas(run_query, get_aqi, get_temperatura)
# exercicio6_receita_por_continente(run_query, get_populacao)
# exercicio7_tempo_medio(run_query, get_temperatura)
# exercicio8_perfil_clima(run_query, get_aqi, get_temperatura)
# exercicio9_exportar_excel(run_query, get_aqi, get_temperatura)