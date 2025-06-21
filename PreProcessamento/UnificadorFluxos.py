import pymongo
import time
import os
import subprocess
from FluxoFile import FluxoFile
from datetime import datetime

# Lista de arquivos a serem processados (caida01 já está no banco)
FILES_FLUXOS = [
    'caida02.txt', 'caida03.txt', 'caida04.txt', 'caida05.txt', 'caida06.txt',
    'caida07.txt', 'caida08.txt', 'caida09.txt', 'caida10.txt', 'caida11.txt',
    'caida12.txt', 'caida13.txt', 'caida14.txt', 'caida15.txt', 'caida16.txt',
]

# Timestamps reais (em segundos)
PCAP_TIMESTAMPS = {
    "caida01.txt": (1547729950.467105000, 1547729999.999996000),  # Já no banco
    "caida02.txt": (1547730000.000000000, 1547730059.999991000),
    "caida03.txt": (1547730060.000000000, 1547730119.999998000),
    "caida04.txt": (1547730120.000004000, 1547730179.999998000),
    "caida05.txt": (1547730180.000004000, 1547730239.999999000),
    "caida06.txt": (1547730240.000001000, 1547730299.999999000),
    "caida07.txt": (1547730300.000001000, 1547730359.999999000),
    "caida08.txt": (1547730360.000000000, 1547730419.999998000),
    "caida09.txt": (1547730420.000000000, 1547730479.999999000),
    "caida10.txt": (1547730480.000000000, 1547730539.999998000),
    "caida11.txt": (1547730540.000002000, 1547730599.999993000),
    "caida12.txt": (1547730600.000003000, 1547730659.999999000),
    "caida13.txt": (1547730660.000000000, 1547730719.999997000),
    "caida14.txt": (1547730707.672897000, 1547730719.999997000),
    "caida15.txt": (1547730720.000004000, 1547730779.999998000),
    "caida16.txt": (1547730780.000004000, 1547730839.999999000),
}

# Conversão para ms
def seconds_to_millis(ts):
    return int(ts * 1000)

# Base para cálculo de offset real
base_ts = PCAP_TIMESTAMPS["caida01.txt"][0]
REAL_OFFSETS = {
    fname: seconds_to_millis(start_ts - base_ts)
    for fname, (start_ts, _) in PCAP_TIMESTAMPS.items()
}

# Parâmetros
DATA_BASE_NAME = "fluxos_database"
COLLECTION_NAME = "caida_collection"
TIMEOUT_LIMIT = 20 * 1000  # 20s em ms
BATCH_SIZE = 500000

# Log formatado
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# Atualiza ou insere novo fluxo
def atualizar_ou_inserir_fluxo(flow, result, offset):
    flow_start_abs = offset + flow.start
    flow_end_abs = flow_start_abs + flow.duration
    final = result['start'] + result['duration']
    time_to_end = flow_start_abs - final

    if time_to_end > TIMEOUT_LIMIT:
        return "insert", None
    else:
        novo_fim = max(final, flow_end_abs)
        new_duration = novo_fim - result['start']
        return "update", new_duration

# Conecta ao MongoDB
with pymongo.MongoClient("mongodb://localhost:27017/") as mongo_client:
    db = mongo_client[DATA_BASE_NAME]
    collection = db[COLLECTION_NAME]

    # Índices
    collection.create_index([
        ("src", pymongo.ASCENDING),
        ("src_port", pymongo.ASCENDING),
        ("dst", pymongo.ASCENDING),
        ("dst_port", pymongo.ASCENDING),
        ("start", pymongo.DESCENDING),
    ])

    log(f"Conectado à base: {db.name}, coleção: {collection.name}")

    for file_name in FILES_FLUXOS:
        full_path = f"./Datasets/Fluxos/CAIDA/{file_name}"
        if not os.path.isfile(full_path):
            log(f"Arquivo não encontrado: {full_path}, pulando...")
            continue

        actual_offset = REAL_OFFSETS[file_name]
        log(f"\nProcessando {file_name} com offset real de {actual_offset}ms")
        start_time = time.time()

        total_inserted = 0
        total_updated = 0
        bulk_operations = []

        with open(full_path, "r") as file:
            for line in file:
                try:
                    flow = FluxoFile(line, True)
                except Exception as e:
                    log(f"Erro ao processar linha: {e}")
                    continue

                query = {
                    "src": flow.src,
                    "src_port": flow.src_port,
                    "dst": flow.dst,
                    "dst_port": flow.dst_port,
                }

                result = collection.find_one(query, sort=[("start", pymongo.DESCENDING)])

                if result:
                    action, new_duration = atualizar_ou_inserir_fluxo(flow, result, actual_offset)
                    if action == "update":
                        total_updated += 1
                        bulk_operations.append(
                            pymongo.UpdateOne(
                                {"_id": result["_id"]},
                                {"$set": {"duration": new_duration}}
                            )
                        )
                    else:
                        total_inserted += 1
                        flow.start += actual_offset
                        bulk_operations.append(pymongo.InsertOne(flow.to_dict()))
                else:
                    total_inserted += 1
                    flow.start += actual_offset
                    bulk_operations.append(pymongo.InsertOne(flow.to_dict()))

                if len(bulk_operations) >= BATCH_SIZE:
                    collection.bulk_write(bulk_operations)
                    bulk_operations = []
                    log(f"{BATCH_SIZE} operações enviadas ao MongoDB")

            if bulk_operations:
                collection.bulk_write(bulk_operations)

        duration = time.time() - start_time
        log(f"Arquivo {file_name} processado em {duration:.2f}s")
        log(f"→ Fluxos inseridos: {total_inserted}")
        log(f"→ Fluxos atualizados: {total_updated}")

# Gera gráficos
log("Gerando gráficos com GeraGraficosMongo.py...")
subprocess.run(["python3", "-u", "./AvaliadorFluxo/GeraGraficosMongo.py"])
