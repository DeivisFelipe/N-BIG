from pymongo import MongoClient
import pandas as pd

# Conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fluxos_database"]  # Altere se necessário
collection = db["mawi_collection"]  # Altere se necessário

# Pipeline de agregação
pipeline = [
    {"$match": {"duration": {"$gt": 0}}},  # evita divisão por zero
    {"$addFields": {
        "rate": {
            "$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]  # bytes / segundos
        }
    }},
    {"$sort": {"rate": -1}},  # ordena do maior para o menor
    {"$limit": 5}  # opcional: limita para os 100 maiores
]

# Executa a agregação
results = list(collection.aggregate(pipeline))

# Converte para DataFrame
df = pd.DataFrame(results)

# Seleciona colunas relevantes
colunas = ["_id", "npackets_total", "nbytes_total", "duration", "rate"]
df = df[colunas]

# Salva em CSV
df.to_csv("top_rates.csv", index=False)

print("Arquivo 'top_rates.csv' criado com sucesso.")
