import pymongo
import time
from FluxoFile import FluxoFile
from datetime import datetime
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Hiperparâmetros
PERMITIR_IPV6 = True
BATCH_SIZE = 1000000

file_name = "E:/mawi2019.txt"

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

db = mongo_client["fluxos_database"] # Cria a base de dados "fluxos_database" se ela não existir

collection = db["mawi2025_collection"] # Cria a coleção "mawi2025_collection" se ela não existir

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

log("Conectando ao MongoDB...")
log("Base de dados: " + db.name)
log("Coleção: " + collection.name)
log("Arquivo: " + file_name)

# Verifica se a coleção tem algum dado, se tiver mostra uma mensagem e cancela a exec
if collection.count_documents({}) > 0:
    log("A coleção já possui dados")

    # Pergunta se deseja limpar a coleção
    resposta = input("Deseja limpar a coleção? (s/n): ").strip().lower()
    if resposta == 's':
        collection.drop()  # Limpa a coleção
        log("Coleção limpa.")
    else:
        log("A execução foi cancelada.")
        mongo_client.close()
        exit()

# Pega o tempo inicial
start_time = time.time()

# Printa o início do processo
log("Inserindo os fluxos no banco de dados...")
log(f"Horário: {time.strftime("%H:%M:%S", time.localtime(start_time))}")

batch = []

with open(file_name, "r") as file:
    for line in file:
        # 23.36.44.166:443 <-> 163.33.141.15:52079          0 0 bytes      36136 2385012 bytes      36136 2385012 bytes 0,000000  71,916941
        fluxo = FluxoFile(line, permitir_ipv6=PERMITIR_IPV6)

        if not PERMITIR_IPV6 and fluxo.ipv6:
            continue

        # Cria um dicionário com os dados do fluxo
        fluxo_dict = fluxo.to_dict()

        # Adiciona o dicionário ao lote
        batch.append(fluxo_dict)

        # Insere o dicionário na coleção
        if len(batch) == BATCH_SIZE:
            collection.insert_many(batch)
            batch = []
            print(f"Fluxos inseridos: {BATCH_SIZE}")

    # Insere o restante dos fluxos
    if batch:
        collection.insert_many(batch)

# Pega o tempo final
final_time = time.time()

# Calcula o tempo de execução
execution_time = final_time - start_time

log(f"Tempo de execução: {execution_time} segundos")
log(f"Tamanho da coleção: {collection.count_documents({})} documentos")