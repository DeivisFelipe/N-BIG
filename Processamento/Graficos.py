from datetime import datetime
import matplotlib.pyplot as plt
import pymongo
import os
import time

# Configurações
DATABASE = 3  # 1 para CAIDA, 2 para MAWI, 3 para MAWI 2025

if DATABASE == 2:
    PATH_GRAPHS = "/Saida/Graficos/AnaliseCaida/GraficosRelacoes"
    NAME = "CAIDA 2019"
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "caida_collection"
elif DATABASE == 2:
    PATH_GRAPHS = "/Saida/Graficos/AnaliseMAWI/GraficosRelacoes"
    NAME = "MAWI 2019"
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "mawi_collection"
elif DATABASE == 3:
    PATH_GRAPHS = "/Saida/Graficos/AnaliseMAWI2025/GraficosRelacoes"
    NAME = "MAWI 2025"
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "mawi2025_collection"
else:
    raise ValueError("Banco de dados inválido. Use 1 para CAIDA, 2 para MAWI 2019 ou 3 para MAWI 2025.")

NUMBER_BINS = 60
print(os.makedirs(PATH_GRAPHS, exist_ok=True))

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def main():
    log("Conectando ao MongoDB...")
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    collection = client[DB_NAME][COLLECTION_NAME]

    log("Calculando histogramas de duração...")
    generate_duration_histograms(collection)

    log("Histogramas de duração finalizados.")
    log("Calculando histogramas de volume...")
    generate_volume_histograms(collection)

    log("Todos os gráficos foram gerados com sucesso.")

def generate_duration_histograms(collection):
    min_dur = collection.find_one(sort=[("duration", 1)])["duration"]
    max_dur = collection.find_one(sort=[("duration", -1)])["duration"]
    step = (max_dur - min_dur) / NUMBER_BINS

    pipeline = [
        {
            "$bucket": {
                "groupBy": "$duration",
                "boundaries": [min_dur + i * step for i in range(NUMBER_BINS)] + [max_dur + 1],
                "default": "out_of_range",
                "output": {
                    "count": {"$sum": 1},
                    "total_packets": {"$sum": "$npackets_total"},
                    "total_bytes": {"$sum": "$nbytes_total"},
                }
            }
        }
    ]

    buckets = list(collection.aggregate(pipeline))

    centers = [round(min_dur + (i + 0.5) * step) for i in range(NUMBER_BINS)]
    counts = []
    avg_pkt_size = []

    for b in buckets:
        count = b["count"]
        total_packets = b["total_packets"]
        total_bytes = b["total_bytes"]
        counts.append(count)
        avg_pkt_size.append(total_bytes / total_packets if total_packets else 0)

    log("Gerando gráfico de linha - Número de fluxos por duração...")
    plt.figure(figsize=(10, 5))
    plt.plot(centers, counts)
    plt.xlabel("Duração (ms)")
    plt.ylabel("Quantidade de fluxos")
    plt.title("Quantidade de fluxos por duração - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/NumeroDeFluxosPorDuracaoLinha.png")
    plt.close()

    log("Gerando gráfico de barras - Número de fluxos por duração...")
    plt.figure(figsize=(10, 5))
    plt.bar(centers, counts, width=step * 0.8, color='blue')
    plt.xlabel("Duração (ms)")
    plt.ylabel("Quantidade de fluxos")
    plt.title("Quantidade de fluxos por duração - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/NumeroDeFluxosPorDuracaoBarra.png")
    plt.close()

    log("Gerando gráfico de linha - Tamanho médio dos pacotes por duração...")
    plt.figure(figsize=(10, 5))
    plt.plot(centers, avg_pkt_size)
    plt.xlabel("Duração (ms)")
    plt.ylabel("Tamanho médio de pacote (bytes)")
    plt.title("Tamanho médio dos pacotes por duração - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/TamanhoMedioPacotesPorDuracaoLinha.png")
    plt.close()

    log("Gerando gráfico de barras - Tamanho médio dos pacotes por duração...")
    plt.figure(figsize=(10, 5))
    plt.bar(centers, avg_pkt_size, width=step * 0.8, color='blue')
    plt.xlabel("Duração (ms)")
    plt.ylabel("Tamanho médio de pacote (bytes)")
    plt.title("Tamanho médio dos pacotes por duração - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/TamanhoMedioPacotesPorDuracaoBarra.png")
    plt.close()

def generate_volume_histograms(collection):
    min_bytes = collection.find_one(sort=[("nbytes_total", 1)])["nbytes_total"]
    max_bytes = collection.find_one(sort=[("nbytes_total", -1)])["nbytes_total"]
    step = (max_bytes - min_bytes) / NUMBER_BINS

    boundaries = [min_bytes + i * step for i in range(NUMBER_BINS)] + [max_bytes + 1]
    centers = [round(min_bytes + (i + 0.5) * step) for i in range(NUMBER_BINS)]
    counts = [0] * NUMBER_BINS  # Inicializa todos os buckets com zero

    pipeline = [
        {
            "$bucket": {
                "groupBy": "$nbytes_total",
                "boundaries": boundaries,
                "default": "out_of_range",
                "output": {"count": {"$sum": 1}}
            }
        }
    ]

    buckets = list(collection.aggregate(pipeline))

    # Preenche os counts com base na posição correta de cada bucket
    for i, bucket in enumerate(buckets):
        if bucket["_id"] != "out_of_range":
            index = boundaries.index(bucket["_id"])
            if index < NUMBER_BINS:
                counts[index] = bucket["count"]

    log("Gerando gráfico de linha - Fluxos por volume...")
    plt.figure(figsize=(10, 5))
    plt.plot(centers, counts)
    plt.yscale("log")
    plt.xlabel("Volume de dados (bytes)")
    plt.ylabel("Quantidade de fluxos")
    plt.title("Quantidade de fluxos por volume - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/NumeroFluxosPorBytesLinha.png")
    plt.close()

    log("Gerando gráfico de barras - Fluxos por volume...")
    plt.figure(figsize=(10, 5))
    plt.bar(centers, counts, width=step * 0.8, color='blue')
    plt.yscale("log")
    plt.xlabel("Volume de dados (bytes)")
    plt.ylabel("Quantidade de fluxos")
    plt.title("Quantidade de fluxos por volume - " + NAME)
    plt.savefig(f"{PATH_GRAPHS}/NumeroFluxosPorBytesBarras.png")
    plt.close()

if __name__ == "__main__":
    start = time.time()
    log("Iniciando a geração dos gráficos...")
    log("=" * 50)
    main()
    log("=" * 50)
    log(f"Tempo total: {round(time.time() - start, 2)} segundos.")
    log("Processamento concluído.")