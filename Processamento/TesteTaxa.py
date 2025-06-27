from pymongo import MongoClient
from pprint import pprint

# Configuração
DATABASE = 2  # 1 para CAIDA, 2 para MAWI

if DATABASE == 1:
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "caida_collection"
elif DATABASE == 2:
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "mawi_collection"
else:
    raise ValueError("Escolha 1 (CAIDA) ou 2 (MAWI)")

# Thresholds fixos (ajuste se quiser usar os dinâmicos calculados antes)
CARACOL_RATE_THRESHOLD = 16384  # 16 KB/s
CHITA_RATE_THRESHOLD = 279265.20  # valor que saiu no seu script principal
MIN_PACKETS = 3

# Conexão
client = MongoClient("mongodb://localhost:27017/")
collection = client[DB_NAME][COLLECTION_NAME]

# Pipeline de teste
pipeline = [
    {"$match": {
        "nbytes_total": {"$gt": 0},
        "duration": {"$gt": 0}
    }},
    {"$limit": 1000},  # Para teste rápido, aumente se quiser
    {"$project": {
        "nbytes_total": 1,
        "duration": 1,
        "npackets_total": 1,
        "rate": {
            "$cond": [
                {"$gt": ["$duration", 0]},
                {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
                0
            ]
        },
        "tipo_taxa": {
            "$cond": [
                {"$lt": ["$npackets_total", MIN_PACKETS]},
                "Normal",
                {
                    "$switch": {
                        "branches": [
                            {"case": {
                                "$lt": [
                                    {
                                        "$cond": [
                                            {"$gt": ["$duration", 0]},
                                            {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
                                            0
                                        ]
                                    }, CARACOL_RATE_THRESHOLD
                                ]
                            }, "then": "Caracol"},
                            {"case": {
                                "$gte": [
                                    {
                                        "$cond": [
                                            {"$gt": ["$duration", 0]},
                                            {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
                                            0
                                        ]
                                    }, CHITA_RATE_THRESHOLD
                                ]
                            }, "then": "Chita"},
                        ],
                        "default": "Normal"
                    }
                }
            ]
        }
    }}
]

# Executa
fluxos = list(collection.aggregate(pipeline))

print("\nClassificação dos Fluxos:")
for f in fluxos:
    rate = f['rate']
    tipo = f['tipo_taxa']
    print(f"rate: {rate:.2f} B/s | duration: {f['duration']} ms | bytes: {f['nbytes_total']} | npackets: {f['npackets_total']} → {tipo}")

# Verificação extra: inconsistência
print("\n🔍 Verificando possíveis erros de classificação:")
for f in fluxos:
    if f['npackets_total'] >= MIN_PACKETS:
        if f['rate'] < CARACOL_RATE_THRESHOLD and f['tipo_taxa'] != "Caracol":
            print(f"❌ Deveria ser 'Caracol': rate={f['rate']:.2f} B/s, classificado como {f['tipo_taxa']}")
        elif f['rate'] >= CHITA_RATE_THRESHOLD and f['tipo_taxa'] != "Chita":
            print(f"❌ Deveria ser 'Chita': rate={f['rate']:.2f} B/s, classificado como {f['tipo_taxa']}")
        elif CARACOL_RATE_THRESHOLD <= f['rate'] < CHITA_RATE_THRESHOLD and f['tipo_taxa'] != "Normal":
            print(f"❌ Deveria ser 'Normal': rate={f['rate']:.2f} B/s, classificado como {f['tipo_taxa']}")
