from datetime import datetime
import matplotlib.pyplot as plt
import pymongo
import pandas as pd
import os

# Configurações gerais
DATABASE = 2  # 1 para CAIDA, 2 para MAWI

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

# Hiperparâmetros (ajuste conforme necessidade)
RATO_THRESHOLD = 160          # 1 KB
LIBELULA_THRESHOLD = 1000            # 1 segundo em ms
CARACOL_RATE_THRESHOLD = 10 * 1024 * 1024  # ex: 10 MB/s (bytes por segundo); ajuste conforme contexto
                                         # lembre-se: taxa = nbytes_total / (duration/1000)
MINIMUM_NPACKETS = 3  # mínimo de pacotes para considerar classificação

# Conexão MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Logging simples
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# 1) Calcular médias e desvios padrão necessários:
#    - média+3σ de nbytes_total (para Elefante)
#    - média+3σ de duration (para Tartaruga)
#    - média+3σ de npackets_total (para Caracol)
stats = collection.aggregate([
    {"$group": {
        "_id": None,
        "avg_bytes": {"$avg": "$nbytes_total"},
        "std_bytes": {"$stdDevPop": "$nbytes_total"},
        "avg_duration": {"$avg": "$duration"},
        "std_duration": {"$stdDevPop": "$duration"},
        "avg_packets": {"$avg": "$npackets_total"},
        "std_packets": {"$stdDevPop": "$npackets_total"}
    }}
])
res = next(stats, None)
if res is None:
    raise RuntimeError("Coleção vazia ou erro ao agregar estatísticas.")
elefante_thresh = res["avg_bytes"] + 3 * res["std_bytes"]
tartaruga_thresh = res["avg_duration"] + 3 * res["std_duration"]
chita_thresh = res["avg_packets"] + 3 * res["std_packets"]

log("Thresholds calculados:")
log(f"  Elefante ≥ {elefante_thresh:.2f} bytes; ")
log(f"  Rato < {RATO_THRESHOLD} bytes; ")
log(f"  Tartaruga ≥ {tartaruga_thresh:.2f} ms; ")
log(f"  Libélula < {LIBELULA_THRESHOLD} ms; ")
log(f"  Caracol < taxa {CARACOL_RATE_THRESHOLD} B/s; ")
log(f"  Chita ≥ taxa {chita_thresh:.2f} B/s")
log("Médias: ")
log(f"  nbytes_total: {res['avg_bytes']:.2f} bytes")
log(f"  duration: {res['avg_duration']:.2f} ms")
log(f"  npackets_total: {res['avg_packets']:.2f} pacotes")
log("Desvios padrão: ")
log(f"  nbytes_total: {res['std_bytes']:.2f} bytes")
log(f"  duration: {res['std_duration']:.2f} ms")    
log(f"  npackets_total: {res['std_packets']:.2f} pacotes")

# 2) Montar pipeline de agregação:
pipeline = [
    # Project: manter campos e calcular taxa de transmissão em bytes/s
    {
        "$project": {
            "duration": 1,
            "nbytes_total": 1,
            "npackets_total": 1,
            "rate": {
                "$cond": [
                    {"$gt": ["$duration", 0]},
                    {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]}, 
                    0
                ]
            },
            # classificação volume
            "tipo_volume": {
                "$cond": [
                    {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
                    "Normal",
                    {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$nbytes_total", elefante_thresh]}, "then": "Elefante"},
                                {"case": {"$lt": ["$nbytes_total", RATO_THRESHOLD]}, "then": "Rato"},
                            ],
                            "default": "Normal"
                        }
                    }
                ]
            },
            # classificação duração
            "tipo_duracao": {
                "$cond": [
                    {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
                    "Normal",
                    {
                        "$switch": {
                            "branches": [
                                {"case": {"$lt": ["$duration", LIBELULA_THRESHOLD]}, "then": "Libélula"},
                                {"case": {"$gte": ["$duration", tartaruga_thresh]}, "then": "Tartaruga"},
                            ],
                            "default": "Normal"
                        }
                    }
                ]
            },
            # classificação pacotes/taxa
            "tipo_pacote": {
                "$cond": [
                    {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
                    "Normal",
                    {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$rate", chita_thresh]}, "then": "Chita"},
                                {"case": {"$gte": ["$rate", CARACOL_RATE_THRESHOLD]}, "then": "Caracol"},
                            ],
                            "default": "Normal"
                        }
                    }
                ]
            }
        }
    },
    # Facet: obter contagens e médias por categoria
    {"$facet": {
        # total de fluxos
        "total_fluxos": [{"$count": "count"}],
        # contagem por volume
        "contagem_volume": [{"$group": {"_id": "$tipo_volume", "count": {"$sum": 1}}}],
        # contagem por duração
        "contagem_duracao": [{"$group": {"_id": "$tipo_duracao", "count": {"$sum": 1}}}],
        # contagem por pacotes/taxa
        "contagem_pacote": [{"$group": {"_id": "$tipo_pacote", "count": {"$sum": 1}}}],
        # médias por volume
        "medias_volume": [
            {"$group": {
                "_id": "$tipo_volume",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"},
                "media_rate": {"$avg": "$rate"}
            }}
        ],
        # médias por duração
        "medias_duracao": [
            {"$group": {
                "_id": "$tipo_duracao",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"},
                "media_rate": {"$avg": "$rate"}
            }}
        ],
        # médias por pacotes/taxa
        "medias_pacote": [
            {"$group": {
                "_id": "$tipo_pacote",
                "media_duration": {"$avg": "$duration"},
                "media_bytes": {"$avg": "$nbytes_total"},
                "media_packets": {"$avg": "$npackets_total"},
                "media_rate": {"$avg": "$rate"}
            }}
        ],
    }}
]

log("Executando agregação com pipeline...")
result = list(collection.aggregate(pipeline))[0]

def facet_to_df(facet_result):
    # converte lista de documentos {"_id": categoria, ...} em DataFrame
    if not facet_result:
        return pd.DataFrame(columns=["Categoria", "count"])  # ou colunas de médias conforme contexto
    df = pd.DataFrame(facet_result).rename(columns={"_id": "Categoria"})
    return df

# Extrair total de fluxos
total_fluxos = result["total_fluxos"][0]["count"] if result["total_fluxos"] else 0
log(f"Total de fluxos: {total_fluxos}")

# DataFrames de contagem
df_volume = facet_to_df(result["contagem_volume"])
df_duracao = facet_to_df(result["contagem_duracao"])
df_pacote = facet_to_df(result["contagem_pacote"])

# DataFrames de médias
df_medias_volume = facet_to_df(result["medias_volume"])
df_medias_duracao = facet_to_df(result["medias_duracao"])
df_medias_pacote = facet_to_df(result["medias_pacote"])

# Salvar CSVs de contagem
df_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Volume.csv"), index=False)
df_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Duracao.csv"), index=False)
df_pacote.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Pacote.csv"), index=False)

# Salvar CSVs de médias
df_medias_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Volume.csv"), index=False)
df_medias_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Duracao.csv"), index=False)
df_medias_pacote.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Pacote.csv"), index=False)

log("Tabelas CSV de contagem e médias geradas.")

# 3) Gerar gráficos de pizza
def plot_pie_two(df_counts, cat1, cat2, title, filename):
    # Gráfico apenas com duas categorias: filtrar df_counts por cat1 e cat2
    df2 = df_counts[df_counts["Categoria"].isin([cat1, cat2])]
    if df2.empty:
        log(f"Atenção: não há dados para categorias {cat1} e {cat2}")
        return
    plt.figure(figsize=(6,6))
    plt.pie(df2["count"], labels=df2["Categoria"], autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.savefig(os.path.join(PATH_GRAPHS, f"{today_str}_{filename}.png"))
    plt.close()

def plot_pie_three(df_counts, title, filename):
    # Gráfico com as três categorias existentes em df_counts
    plt.figure(figsize=(6,6))
    plt.pie(df_counts["count"], labels=df_counts["Categoria"], autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.savefig(os.path.join(PATH_GRAPHS, f"{today_str}_{filename}.png"))
    plt.close()

# Volume: Elefante vs Rato
plot_pie_two(df_volume, "Elefante", "Rato",
             f"Proporção Elefante vs Rato - {NAME}", "Pie_Elefante_Rato")
# Volume: Elefante vs Rato vs Normal
plot_pie_three(df_volume,
               f"Proporção Elefante/Rato/Normal - {NAME}", "Pie_Volume_Todas")

# Duração: Libélula vs Tartaruga
plot_pie_two(df_duracao, "Libélula", "Tartaruga",
             f"Proporção Libélula vs Tartaruga - {NAME}", "Pie_Libelula_Tartaruga")
# Duração: Libélula vs Tartaruga vs Normal
plot_pie_three(df_duracao,
               f"Proporção Libélula/Tartaruga/Normal - {NAME}", "Pie_Duracao_Todas")

# Pacotes/Taxa: Chita vs Caracol
plot_pie_two(df_pacote, "Chita", "Caracol",
             f"Proporção Chita vs Caracol - {NAME}", "Pie_Chita_Caracol")
# Pacotes/Taxa: Chita vs Caracol vs Normal
plot_pie_three(df_pacote,
               f"Proporção Chita/Caracol/Normal - {NAME}", "Pie_Pacote_Todas")

log("Gráficos de pizza gerados e salvos.")