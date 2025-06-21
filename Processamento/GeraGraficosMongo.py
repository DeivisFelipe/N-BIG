import matplotlib.pyplot as plt
import pymongo
import time

# Configurações
DATABASE = 1 # 1 para CAIDA, 2 para MAWI

if DATABASE == 1:
    PATH_GRAPHS = "Saida/Graficos/AnaliseCaida"
    NAME = "CAIDA MongoDB"
    NUMBER_BINS_HISTOGRAMA = 60
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "caida_collection"
elif DATABASE == 2:
    PATH_GRAPHS = "Saida/Graficos/AnaliseMAWI"
    NAME = "MAWI MongoDB"
    NUMBER_BINS_HISTOGRAMA = 60
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "mawi_collection"
else:
    raise ValueError("Banco de dados inválido. Use 1 para CAIDA ou 2 para MAWI.")

def main():
    # Conecta ao MongoDB
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Pega o maior e o menor tempo de duracao e a maior e a menor quantidade de bytes usando queries
    bigger_duration = collection.find_one(sort=[("duration", pymongo.DESCENDING)])["duration"]
    smaller_duration = collection.find_one(sort=[("duration", pymongo.ASCENDING)])["duration"]
    bigger_bytes = collection.find_one(sort=[("nbytes_total", pymongo.DESCENDING)])["nbytes_total"]
    smaller_bytes = collection.find_one(sort=[("nbytes_total", pymongo.ASCENDING)])["nbytes_total"]

    print("Maior duração: ", bigger_duration)
    print("Menor duração: ", smaller_duration)
    print("Maior quantidade de bytes: ", bigger_bytes)
    print("Menor quantidade de bytes: ", smaller_bytes)

    # Gera os intervalos de duracao e bytes
    duration_interval = (bigger_duration - smaller_duration) / NUMBER_BINS_HISTOGRAMA
    duration_intervals = []
    for i in range(NUMBER_BINS_HISTOGRAMA):
        duration_intervals.append(round(smaller_duration + i * duration_interval))

    bytes_interval = (bigger_bytes - smaller_bytes) / NUMBER_BINS_HISTOGRAMA
    bytes_intervals = []
    for i in range(NUMBER_BINS_HISTOGRAMA):
        bytes_intervals.append(round(smaller_bytes + i * bytes_interval))

    # Contadores
    flows_by_duration_counters = [0 for i in range(NUMBER_BINS_HISTOGRAMA)]
    flows_by_bytes_counters = [0 for i in range(NUMBER_BINS_HISTOGRAMA)]
    packets_by_duration_counters = [0 for i in range(NUMBER_BINS_HISTOGRAMA)]
    total_bytes_by_duration_counters = [0 for i in range(NUMBER_BINS_HISTOGRAMA)]

    duration_histogram(collection, duration_intervals, flows_by_duration_counters, packets_by_duration_counters, total_bytes_by_duration_counters)
    bytes_histogram(collection, bytes_intervals, flows_by_bytes_counters)
    average_packet_size_by_duration_histogram(duration_intervals, packets_by_duration_counters, total_bytes_by_duration_counters)

# Graficos

# Histograma de duracao dos fluxos
def duration_histogram(collection, duration_intervals, flows_by_duration_counters, packets_by_duration_counters, total_bytes_by_duration_counters):
    print("*" * 50)
    print("Gerando histograma de duração dos fluxos...")
    for i in range(NUMBER_BINS_HISTOGRAMA):
        if i == NUMBER_BINS_HISTOGRAMA - 1:
            query = {"duration": {"$gte": duration_intervals[i]}}
        else:
            query = {"duration": {"$gte": duration_intervals[i], "$lt": duration_intervals[i + 1]}}
        flows_by_duration_counters[i] = collection.count_documents(query)

        # Conta a quantidade de pacotes e bytes
        # Verifique se ha resultado antes de acessar
        result = collection.aggregate([
            {"$match": query},
            {"$group": {"_id": None, "total_packets": {"$sum": "$npackets_total"}, "total_bytes": {"$sum": "$nbytes_total"}}}
        ])
        result = next(result, None)
        if result:
            packets_by_duration_counters[i] = result["total_packets"]
            total_bytes_by_duration_counters[i] = result["total_bytes"]
        else:
            packets_by_duration_counters[i] = 0
            total_bytes_by_duration_counters[i] = 0

    # Grafico de linha
    plt.figure(figsize=(10, 5))
    plt.plot(duration_intervals, flows_by_duration_counters)
    plt.xlabel('Intervalos de duração')
    plt.ylabel('Quantidade de fluxos')
    plt.title('Quantidade de fluxos por duração - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/NumeroDeFluxosPorDuracaoLinha.png")

    # Gŕafico de barras
    plt.figure(figsize=(10, 5))
    plt.bar(duration_intervals, flows_by_duration_counters, color="blue", width=(duration_intervals[1] - duration_intervals[0]) * 0.8)
    plt.xlabel('Intervalos de duração')
    plt.ylabel('Quantidade de fluxos')
    plt.title('Quantidade de fluxos por duração - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/NumeroDeFluxosPorDuracaoBarra.png")
    print("Quantidade de fluxos por duracao gerado com sucesso!")

def bytes_histogram(collection, bytes_intervals, flows_by_bytes_counters):
    print("*" * 50)
    print("Gerando histograma de bytes dos fluxos...")
    for i in range(NUMBER_BINS_HISTOGRAMA):
        if i == NUMBER_BINS_HISTOGRAMA - 1:
            query = {"nbytes_total": {"$gte": bytes_intervals[i]}}
        else:
            query = {"nbytes_total": {"$gte": bytes_intervals[i], "$lt": bytes_intervals[i + 1]}}
        flows_by_bytes_counters[i] = collection.count_documents(query)

    # Grafico de linha
    plt.figure(figsize=(10, 5))
    plt.plot(bytes_intervals, flows_by_bytes_counters)
    plt.yscale('log')
    plt.xlabel('Intervalos de bytes')
    plt.ylabel('Quantidade de fluxos')
    plt.title('Quantidade de fluxos por bytes - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/NumeroFluxosPorBytesLinha.png")

    # Grafico de barra
    plt.figure(figsize=(10, 5))
    plt.bar(bytes_intervals, flows_by_bytes_counters, color="blue", width=(bytes_intervals[1] - bytes_intervals[0]) * 0.8)
    plt.yscale('log')
    plt.xlabel('Intervalos de bytes')
    plt.ylabel('Quantidade de fluxos')
    plt.title('Quantidade de fluxos por bytes - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/NumeroFluxosPorBytesBarras.png")
    print("Quantidade de fluxos por bytes gerado com sucesso!")

def average_packet_size_by_duration_histogram(duration_intervals, packets_by_duration_counters, total_bytes_by_duration_counters):
    print("*" * 50)
    print("Gerando histograma de tamanho médio dos pacotes por duração...")
    tamanho_medio = []
    for i in range(NUMBER_BINS_HISTOGRAMA):
        if packets_by_duration_counters[i] != 0:
            tamanho_medio.append(total_bytes_by_duration_counters[i] / packets_by_duration_counters[i])
        else:
            tamanho_medio.append(0)

    # Gráfico de linha
    plt.clf()
    plt.plot(duration_intervals, tamanho_medio) 
    plt.xlabel('Intervalos de duração')
    plt.ylabel('Tamanho médio dos pacotes')
    plt.title('Tamanho médio dos pacotes em relação a duração - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/TamanhoMedioPacotesPorDuracaoLinha.png")

    # Gráfico de barras
    plt.clf()
    plt.bar(duration_intervals, tamanho_medio, color="blue", width=(duration_intervals[1] - duration_intervals[0]) * 0.8)
    plt.xlabel('Intervalos de duração')
    plt.ylabel('Tamanho médio dos pacotes')
    plt.title('Tamanho médio dos pacotes em relação a duração - ' + NAME)
    plt.savefig(PATH_GRAPHS + "/TamanhoMedioPacotesPorDuracaoBarra.png")
    print("Histograma de tamanho médio dos pacotes por duração gerado com sucesso!")

if __name__ == '__main__':
    start_time = time.time()
    print("Iniciando a geração dos graficos...")
    print("Horário:", time.strftime("%H:%M:%S", time.localtime(start_time)))
    print("=" * 50)
    main()
    print("=" * 50)
    end_time = time.time()
    execution_time = end_time - start_time
    print("Tempo de execução: ", execution_time, " segundos")