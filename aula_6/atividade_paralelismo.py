import os
import time
import random
import csv
import threading
import requests
import math
import pandas as pd
import numpy as np

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

# ------------------------------------------------------------
# NÍVEL 1 — Fundamentos com foco em I/O
# ------------------------------------------------------------


def nivel1_exercicio1():
    """
    1. Crawler de APIs concorrente com threading.Thread
    - Simula a coleta de dados de 10 endpoints (httpbin.org).
    - Cada thread faz uma requisição GET e armazena status, latência e parte do conteúdo.
    """

    def crawl_endpoint(endpoint_url, resultados, index):
        start = time.time()
        try:
            response = requests.get(endpoint_url, timeout=5)
            latency = time.time() - start
            resultados[index] = {
                "url": endpoint_url,
                "status_code": response.status_code,
                "latency": latency,
                "conteudo": response.text[:100],
            }
            print(
                f"[Thread {index}] {endpoint_url} → status {response.status_code}, tempo {latency:.2f}s"
            )
        except Exception as e:
            latency = time.time() - start
            resultados[index] = {
                "url": endpoint_url,
                "status_code": None,
                "latency": latency,
                "erro": str(e),
            }
            print(f"[Thread {index}] Erro em {endpoint_url}: {e} (após {latency:.2f}s)")

    endpoints = [
        "https://httpbin.org/get?valor=1",
        "https://httpbin.org/get?valor=2",
        "https://httpbin.org/get?valor=3",
        "https://httpbin.org/get?valor=4",
        "https://httpbin.org/get?valor=5",
        "https://httpbin.org/get?valor=6",
        "https://httpbin.org/get?valor=7",
        "https://httpbin.org/get?valor=8",
        "https://httpbin.org/get?valor=9",
        "https://httpbin.org/get?valor=10",
    ]

    threads = []
    resultados = {}

    for i, url in enumerate(endpoints):
        thread = threading.Thread(target=crawl_endpoint, args=(url, resultados, i))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("\n=== RESULTADOS AGREGADOS (Exercício 1) ===")
    for idx, info in resultados.items():
        if info.get("status_code") is not None:
            print(
                f"[{idx}] {info['url']} → status {info['status_code']}, latência {info['latency']:.2f}s"
            )
        else:
            print(f"[{idx}] {info['url']} → ERRO: {info['erro']}")


def nivel1_exercicio2():
    """
    2. Ingestão de múltiplos arquivos CSV com ThreadPoolExecutor
    - Gera 10 CSVs de exemplo (caso não existam).
    - Lê todos em paralelo usando ThreadPoolExecutor e exibe o cabeçalho de cada DataFrame.
    """

    def gerar_csv_exemplo(caminho, num_linhas=5):
        if os.path.exists(caminho):
            return
        df = pd.DataFrame(
            {
                "A": range(1, num_linhas + 1),
                "B": [f"texto_{i}" for i in range(1, num_linhas + 1)],
            }
        )
        df.to_csv(caminho, index=False)

    def ler_csv(caminho):
        df = pd.read_csv(caminho)
        print(f"Lido {caminho}: {len(df)} linhas")
        return df

    pasta_csv = "csv_exemplo"
    os.makedirs(pasta_csv, exist_ok=True)

    caminhos = []
    for i in range(1, 11):
        nome_arquivo = f"arquivo_{i}.csv"
        caminho = os.path.join(pasta_csv, nome_arquivo)
        gerar_csv_exemplo(caminho, num_linhas=10 + i)
        caminhos.append(caminho)

    dataframes = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(ler_csv, caminho) for caminho in caminhos]
        for future in futures:
            dataframes.append(future.result())

    print("\n=== CABEÇALHOS DOS DATAFRAMES LIDOS (Exercício 2) ===")
    for i, df in enumerate(dataframes, start=1):
        print(f"Arquivo {i}:")
        print(df.head(), "\n")


def nivel1_exercicio3():
    """
    3. Monitoramento de tempo de resposta com múltiplas threads
    - Testa o tempo de resposta de 10 URLs em paralelo (httpbin.org).
    - Grava os resultados (url, status_code, latência, erro) em um CSV.
    """

    def testar_url(url, resultados, index):
        start = time.time()
        try:
            resp = requests.get(url, timeout=5)
            latencia = time.time() - start
            resultados[index] = {
                "url": url,
                "status_code": resp.status_code,
                "latencia": latencia,
            }
            print(
                f"[Thread {index}] {url} → status {resp.status_code}, latência {latencia:.2f}s"
            )
        except Exception as e:
            latencia = time.time() - start
            resultados[index] = {
                "url": url,
                "status_code": None,
                "latencia": latencia,
                "erro": str(e),
            }
            print(f"[Thread {index}] Erro em {url}: {e} (após {latencia:.2f}s)")

    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/404",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/get",
        "https://httpbin.org/uuid",
        "https://httpbin.org/bytes/1024",
        "https://httpbin.org/ip",
        "https://httpbin.org/user-agent",
        "https://httpbin.org/headers",
    ]

    resultados = {}
    threads = []

    for i, url in enumerate(urls):
        t = threading.Thread(target=testar_url, args=(url, resultados, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    nome_csv = "monitoramento_tempos.csv"
    with open(nome_csv, mode="w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(["url", "status_code", "latencia", "erro"])
        for idx in range(len(urls)):
            info = resultados[idx]
            escritor.writerow(
                [
                    info.get("url"),
                    info.get("status_code"),
                    f"{info.get('latencia'):.2f}",
                    info.get("erro", ""),
                ]
            )

    print(f"\nResultados gravados em '{nome_csv}'. (Exercício 3)")


def nivel1_exercicio4():
    """
    4. Download concorrente de arquivos (simulado)
    - Simula o download de 10 arquivos “grandes” usando sleep aleatório.
    - Cada tarefa cria um arquivo vazio ao final para indicar “download concluído”.
    """

    def simular_download(nome_arquivo):
        tempo_simulado = random.uniform(1, 5)
        print(
            f"Iniciando download simulado de '{nome_arquivo}' por {tempo_simulado:.2f}s..."
        )
        time.sleep(tempo_simulado)

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            f.write(f"Arquivo '{nome_arquivo}' baixado em {tempo_simulado:.2f}s.\n")
        print(f"Download concluído: {nome_arquivo}")
        return nome_arquivo, tempo_simulado

    nomes = [f"arquivo_grande_{i}.bin" for i in range(1, 11)]
    os.makedirs("downloads_simulados", exist_ok=True)
    caminhos = [os.path.join("downloads_simulados", nm) for nm in nomes]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(simular_download, caminho) for caminho in caminhos]
        for future in futures:
            arquivo, duracao = future.result()
            print(f"→ {arquivo} baixado em {duracao:.2f}s")

    print("\nTodos os downloads simulados foram concluídos. (Exercício 4)")


def nivel1_exercicio5():
    """
    5. Consulta a múltiplas bases de dados (simulada)
    - Simula 5 “conexões” a bancos diferentes, usando sleep aleatório.
    - Cada thread retorna contagem de linhas simulada e latência.
    """

    def consultar_base(nome_base, resultados, index):
        tempo_simulado = random.uniform(0.5, 2.0)
        print(
            f"[{nome_base}] Iniciando consulta (duração simulada: {tempo_simulado:.2f}s)..."
        )
        time.sleep(tempo_simulado)

        contagem_linhas = random.randint(100, 1000)
        resultados[index] = {
            "base": nome_base,
            "linhas_lidas": contagem_linhas,
            "duracao": tempo_simulado,
        }
        print(
            f"[{nome_base}] Consulta concluída: {contagem_linhas} linhas em {tempo_simulado:.2f}s."
        )

    bases = ["DB_Clientes", "DB_Vendas", "DB_Produtos", "DB_Financeiro", "DB_Logistica"]

    resultados = {}
    threads = []

    for i, nome in enumerate(bases):
        t = threading.Thread(target=consultar_base, args=(nome, resultados, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n=== RESUMO DAS CONSULTAS (Exercício 5) ===")
    for idx in range(len(bases)):
        info = resultados[idx]
        print(
            f"{info['base']}: {info['linhas_lidas']} linhas lidas (tempo {info['duracao']:.2f}s)"
        )


# ------------------------------------------------------------
# NÍVEL 2 — Processamento CPU-bound com dados
# ------------------------------------------------------------


def transformacao_pesada(sublista):
    resultado = []
    for x in sublista:
        val = math.sqrt(x) + math.log(x + 1) + (x**2)
        resultado.append(val)
    return resultado


def nivel2_exercicio6():
    """
    6. Transformação de dados pesados com ProcessPoolExecutor
    - Gera 1_000_000 valores aleatórios.
    - Divide em 4 blocos e aplica em cada um uma operação custosa (sqrt + log + x^2).
    """

    tamanho_total = 1_000_000
    dados = [random.uniform(0, 1000) for _ in range(tamanho_total)]

    num_processos = 4
    tamanho_bloco = tamanho_total // num_processos
    blocos = [
        dados[i * tamanho_bloco : (i + 1) * tamanho_bloco] for i in range(num_processos)
    ]
    resto = dados[num_processos * tamanho_bloco :]
    if resto:
        blocos[-1].extend(resto)

    with ProcessPoolExecutor(max_workers=num_processos) as executor:
        futures = [executor.submit(transformacao_pesada, bloco) for bloco in blocos]

        resultados = []
        for future in futures:
            resultados.extend(future.result())

    print(
        f"Transformação concluída. Total de elementos processados: {len(resultados)} (Exercício 6)"
    )


def funcao_complexa(df_slice):
    df = df_slice.copy()
    df["Resultado"] = np.sqrt(df["X"] ** 2 + df["Y"] ** 2) + np.log(
        df["X"] + df["Y"] + 1
    )
    return df


def nivel2_exercicio7():
    """
    7. Paralelizar aplicação de funções complexas em DataFrames
    - Cria um DataFrame de 200.000 linhas com colunas X e Y.
    - Divide em 4 partes e, em cada processo, aplica cálculo: sqrt(X^2+Y^2)+log(X+Y+1).
    - Concatena resultados e mostra as últimas linhas.
    """

    num_linhas = 200_000
    df = pd.DataFrame(
        {
            "X": np.random.uniform(0, 100, size=num_linhas),
            "Y": np.random.uniform(0, 200, size=num_linhas),
        }
    )

    num_processos = 4
    blocos = np.array_split(df, num_processos)

    with ProcessPoolExecutor(max_workers=num_processos) as executor:
        futures = [executor.submit(funcao_complexa, bloco) for bloco in blocos]
        partes_processadas = [future.result() for future in futures]

    df_transformado = pd.concat(partes_processadas, ignore_index=True)
    print("DataFrame transformado. Exemplo de linhas finais (Exercício 7):")
    print(df_transformado.tail())


def gerar_parquet_exemplo(caminho, num_linhas=1000):
    if os.path.exists(caminho):
        return
    df = pd.DataFrame(
        {
            "A": np.random.randint(0, 100, size=num_linhas),
            "B": np.random.random(size=num_linhas),
            "C": np.random.choice(["X", "Y", "Z"], size=num_linhas),
        }
    )
    df.to_parquet(caminho)


def converter_parquet_para_csv(caminho_parquet, caminho_csv):
    df = pd.read_parquet(caminho_parquet)
    df.to_csv(caminho_csv, index=False)
    print(f"Convertido: {caminho_parquet} → {caminho_csv}")


def nivel2_exercicio8():
    """
    8. Conversão paralela de arquivos Parquet → CSV
    - Gera 10 Parquets de exemplo (cada um com 5.000+ linhas).
    - Em paralelo, converte cada Parquet para CSV.
    """
    pasta_parquet = "parquets_exemplo"
    pasta_csv = "csv_resultantes"
    os.makedirs(pasta_parquet, exist_ok=True)
    os.makedirs(pasta_csv, exist_ok=True)

    caminhos_parquet = []
    caminhos_csv = []
    for i in range(1, 11):
        nome_pq = os.path.join(pasta_parquet, f"arquivo_{i}.parquet")
        gerar_parquet_exemplo(nome_pq, num_linhas=5000 + i * 100)
        caminhos_parquet.append(nome_pq)

        nome_csv = os.path.join(pasta_csv, f"arquivo_{i}.csv")
        caminhos_csv.append(nome_csv)

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        for pq, csv_out in zip(caminhos_parquet, caminhos_csv):
            futures.append(executor.submit(converter_parquet_para_csv, pq, csv_out))
        for future in futures:
            future.result()

    print("\nTodas as conversões Parquet → CSV foram concluídas. (Exercício 8)")


def agregacoes_por_bloco(df_slice):
    agg = df_slice.groupby("Chave")["Valor"].agg(["sum", "mean", "std"]).reset_index()
    return agg


def nivel2_exercicio9():
    """
    9. Cálculo de agregações pesadas com multiprocessing
    - Gera um DataFrame com 500.000 linhas, chaves em 10 grupos e valores aleatórios.
    - Divide em 4 blocos, cada processo calcula soma, média e std por grupo no bloco.
    - Em seguida, concatena resultados parciais e faz agregação final por grupo.
    """


    num_linhas = 500_000
    chaves = np.random.choice([f"Grupo_{i}" for i in range(1, 11)], size=num_linhas)
    valores = np.random.random(size=num_linhas) * 100

    df = pd.DataFrame({"Chave": chaves, "Valor": valores})

    num_processos = 4
    blocos = np.array_split(df, num_processos)

    with mp.Pool(processes=num_processos) as pool:
        resultados_parciais = pool.map(agregacoes_por_bloco, blocos)

    df_concat = pd.concat(resultados_parciais, ignore_index=True)
    agg_final = (
        df_concat.groupby("Chave")
        .agg({"sum": "sum", "mean": "mean", "std": "mean"})
        .reset_index()
    )

    print("Agregações finais por grupo (Exercício 9):")
    print(agg_final)


def processo_transformacao(fila_in, fila_out):
    while True:
        bloco = fila_in.get()
        if bloco is None:
            print("[Transformação] Recebeu sinal de término. Encerrando.")
            fila_out.put(None)
            break
        df = bloco.copy()
        df["ValorTransformado"] = df["Valor"] * 2
        print(f"[Transformação] Transformado bloco com {len(df)} linhas.")
        fila_out.put(df)


def processo_persistencia(fila_out, pasta_saida):
    contador = 0
    while True:
        df_transformado = fila_out.get()
        if df_transformado is None:
            print("[Persistência] Recebeu sinal de término. Encerrando.")
            break
        nome_arquivo = os.path.join(pasta_saida, f"bloco_transformado_{contador}.csv")
        df_transformado.to_csv(nome_arquivo, index=False)
        print(
            f"[Persistência] Gravado: {nome_arquivo} ({len(df_transformado)} linhas)."
        )
        contador += 1


def nivel2_exercicio10():
    """
    10. Multiprocessamento em pipelines: transformação + persistência
    - Usa multiprocessing.Process e multiprocessing.Queue.
    - Processo de transformação: recebe DataFrame, multiplica coluna 'Valor' por 2 e envia adiante.
    - Processo de persistência: recebe DataFrame transformado e grava cada bloco em CSV.
    """

    pasta_saida = "blocos_transformados"
    os.makedirs(pasta_saida, exist_ok=True)

    fila_in = mp.Queue()
    fila_out = mp.Queue()

    p_transform = mp.Process(target=processo_transformacao, args=(fila_in, fila_out))
    p_persist = mp.Process(target=processo_persistencia, args=(fila_out, pasta_saida))

    p_transform.start()
    p_persist.start()

    num_blocos = 5
    tamanho_bloco = 20_000

    for i in range(num_blocos):
        df = pd.DataFrame(
            {
                "ID": range(i * tamanho_bloco, (i + 1) * tamanho_bloco),
                "Valor": np.random.random(size=tamanho_bloco) * 100,
            }
        )
        print(
            f"[Principal] Enviando bloco {i} com {len(df)} linhas para transformação."
        )
        fila_in.put(df)

    fila_in.put(None)

    p_transform.join()
    p_persist.join()

    print("\nPipeline de transformação + persistência concluído. (Exercício 10)")


if __name__ == "__main__":
    # nivel1_exercicio1()
    # nivel1_exercicio2()
    # nivel1_exercicio3()
    # nivel1_exercicio4()
    # nivel1_exercicio5()
    # nivel2_exercicio6()
    # nivel2_exercicio7()
    # nivel2_exercicio8()
    # nivel2_exercicio9()
    nivel2_exercicio10()
