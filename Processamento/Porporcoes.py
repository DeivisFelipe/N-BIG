from datetime import datetime
import matplotlib.pyplot as plt
import pymongo
import pandas as pd
import os

# Configurações
DATABASE = 1  # 1 para CAIDA, 2 para MAWI

if DATABASE == 1:
    PATH_GRAPHS = "Saida/Graficos/AnaliseCaida"
    NAME = "CAIDA MongoDB"
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "caida_collection"
elif DATABASE == 2:
    PATH_GRAPHS = "Saida/Graficos/AnaliseMAWI"
    NAME = "MAWI MongoDB"
    DB_NAME = "fluxos_database"
    COLLECTION_NAME = "mawi_collection"
else:
    raise ValueError("Banco de dados inválido. Use 1 para CAIDA ou 2 para MAWI.")

os.makedirs(PATH_GRAPHS, exist_ok=True)
today_str = datetime.now().strftime('%Y%m%d')

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def get_thresholds():
    # Obtém média e desvio padrão de nbytes_total para definir elefantes/ratos
    stats = collection.aggregate([
        {"$group": {
            "_id": None,
            "avg_bytes": {"$avg": "$nbytes_total"},
            "std_bytes": {"$stdDevPop": "$nbytes_total"},
            "avg_duration": {"$avg": "$duration"},
            "avg_packets": {"$avg": "$npackets_total"}
        }}
    ])
    res = next(stats)
    elefante_thresh = res["avg_bytes"] + 3 * res["std_bytes"]
    media_duration = res["avg_duration"]
    media_packets = res["avg_packets"]
    return elefante_thresh, media_duration, media_packets

elefante_thresh, media_duration, media_packets = get_thresholds()

# Helper para classificar tipo volume (elefante/rato)
def classify_volume(value):
    return "Elefante" if value >= elefante_thresh else "Rato"

# Helper para classificar tipo duração (chita/caracol)
def classify_duration(value):
    return "Chita" if value <= media_duration else "Caracol"

# Helper para classificar tipo pacotes (libelula/tartaruga)
def classify_packets(value):
    return "Libélula" if value <= media_packets else "Tartaruga"

# Pipeline para agregar e classificar fluxos com bucket para duração, bytes e pacotes
pipeline = [
    {"$project": {
        "duration": 1,
        "nbytes_total": 1,
        "npackets_total": 1,
        "tipo_volume": {"$cond": [{"$gte": ["$nbytes_total", elefante_thresh]}, "Elefante", "Rato"]},
        "tipo_duracao": {"$cond": [{"$lte": ["$duration", media_duration]}, "Chita", "Caracol"]},
        "tipo_pacote": {"$cond": [{"$lte": ["$npackets_total", media_packets]}, "Libélula", "Tartaruga"]}
    }},
    {"$facet": {
        # Contagem total de fluxos
        "total_fluxos": [{"$count": "count"}],

        # Contagem por tipo volume
        "contagem_volume": [
            {"$group": {"_id": "$tipo_volume", "count": {"$sum": 1}}},
        ],

        # Contagem por tipo duracao
        "contagem_duracao": [
            {"$group": {"_id": "$tipo_duracao", "count": {"$sum": 1}}},
        ],

        # Contagem por tipo pacote
        "contagem_pacote": [
            {"$group": {"_id": "$tipo_pacote", "count": {"$sum": 1}}},
        ],

        # Médias agrupadas por volume
        "medias_volume": [
            {"$group": {
                "_id": "$tipo_volume",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"}
            }}
        ],

        # Médias agrupadas por duracao
        "medias_duracao": [
            {"$group": {
                "_id": "$tipo_duracao",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"}
            }}
        ],

        # Médias agrupadas por pacote
        "medias_pacote": [
            {"$group": {
                "_id": "$tipo_pacote",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"}
            }}
        ],
    }}
]

result = list(collection.aggregate(pipeline))[0]

# Função para converter resultado facet em dataframe
def facet_to_df(facet_result):
    return pd.DataFrame(facet_result).rename(columns={"_id": "Categoria"})

# Total fluxos
total_fluxos = result["total_fluxos"][0]["count"] if result["total_fluxos"] else 0
print(f"Total de fluxos: {total_fluxos}")

# DataFrames para contagens
df_volume = facet_to_df(result["contagem_volume"])
df_duracao = facet_to_df(result["contagem_duracao"])
df_pacote = facet_to_df(result["contagem_pacote"])

# DataFrames para médias
df_medias_volume = facet_to_df(result["medias_volume"])
df_medias_duracao = facet_to_df(result["medias_duracao"])
df_medias_pacote = facet_to_df(result["medias_pacote"])

# Salva tabelas CSV
df_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Volume.csv"), index=False)
df_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Duracao.csv"), index=False)
df_pacote.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Pacote.csv"), index=False)

df_medias_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Volume.csv"), index=False)
df_medias_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Duracao.csv"), index=False)
df_medias_pacote.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Pacote.csv"), index=False)

print("Tabelas de contagem e médias salvas.")

# --- Gráficos de pizza ---
def plot_pie(df_counts, title, filename):
    plt.figure(figsize=(6,6))
    plt.pie(df_counts['count'], labels=df_counts['Categoria'], autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.savefig(os.path.join(PATH_GRAPHS, f"{today_str}_{filename}.png"))
    plt.close()

plot_pie(df_volume, f"Proporção de Fluxos Elefantes/Ratos - {NAME}", "Proporcao_Elefante_Rato")
plot_pie(df_duracao, f"Proporção de Fluxos Chita/Caracol - {NAME}", "Proporcao_Chita_Caracol")
plot_pie(df_pacote, f"Proporção de Fluxos Libélula/Tartaruga - {NAME}", "Proporcao_Libelula_Tartaruga")

print("Gráficos gerados e salvos com sucesso.")
