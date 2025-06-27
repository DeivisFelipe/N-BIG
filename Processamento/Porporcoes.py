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

# Pergunta classificações no terminal
print("Selecione as classificações (separe por vírgula):", flush=True)
print("1 - Volume (Elefante/Rato)", flush=True)
print("2 - Duração (Libélula/Tartaruga)", flush=True)
print("3 - Taxa (Chita/Caracol)", flush=True)
opcoes = input("Digite os números das classificações (ex: 1,3): ")

mapa = {"1": "volume", "2": "duracao", "3": "taxa"}
selecionados = [mapa[o.strip()] for o in opcoes.split(",") if o.strip() in mapa]

print("Classificações selecionadas:", ", ".join(selecionados), flush=True)

# Hiperparâmetros (ajuste conforme necessidade)
RATO_THRESHOLD = 163  # 163 bytes
LIBELULA_THRESHOLD = 1000  # 1 segundo (em ms)
MINIMUM_NPACKETS = 3  # mínimo de pacotes para considerar classificação

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# Estatísticas para cálculo de thresholds
stats_project = {"_id": None}
if "volume" in selecionados:
    stats_project.update({
        "avg_bytes": {"$avg": "$nbytes_total"},
        "std_bytes": {"$stdDevPop": "$nbytes_total"},
    })
if "duracao" in selecionados:
    stats_project.update({
        "avg_duration": {"$avg": "$duration"},
        "std_duration": {"$stdDevPop": "$duration"},
    })
if "taxa" in selecionados:
    stats_project.update({
        "avg_rate": {"$avg": {"$cond": [
            {"$gt": ["$duration", 0]},
            {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
            0
        ]}},
        "std_rate": {"$stdDevPop": {"$cond": [
            {"$gt": ["$duration", 0]},
            {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
            0
        ]}},
    })

# Filtro para garantir taxa válida
match_stage = {
    "$match": {
        "nbytes_total": {"$gt": 0},
        "duration": {"$gt": 0}
    }
}

stats = collection.aggregate([
    match_stage,
    {"$group": stats_project}
])
res = next(stats)

# Cálculo de thresholds com base nos valores reais
elefante_thresh = res.get("avg_bytes", 0) + 3 * res.get("std_bytes", 0)
tartaruga_thresh = res.get("avg_duration", 0) + 3 * res.get("std_duration", 0)
chita_thresh = res.get("avg_rate", 0) + 3 * res.get("std_rate", 0)
CARACOL_RATE_THRESHOLD = res.get("avg_rate", 0) - res.get("std_rate", 0)
if CARACOL_RATE_THRESHOLD < 0:
    CARACOL_RATE_THRESHOLD = 150  # 16KB/s fallback

log("Thresholds calculados:")
log(f"  Elefante ≥ {elefante_thresh:.2f} bytes; ")
log(f"  Rato < {RATO_THRESHOLD} bytes; ")
log(f"  Tartaruga ≥ {tartaruga_thresh:.2f} ms; ")
log(f"  Libélula < {LIBELULA_THRESHOLD} ms; ")
log(f"  Caracol < taxa {CARACOL_RATE_THRESHOLD} B/s; ")
log(f"  Chita ≥ taxa {chita_thresh:.2f} B/s")
log("Médias: ")
log(f"  nbytes_total: {res.get('avg_bytes', 0):.2f} bytes")
log(f"  duration: {res.get('avg_duration', 0):.2f} ms")
log(f"  taxa: {res.get('avg_rate', 0):.2f} B/s")
log("Desvios padrão: ")
log(f"  nbytes_total: {res.get('std_bytes', 0):.2f} bytes")
log(f"  duration: {res.get('std_duration', 0):.2f} ms")    
log(f"  taxa: {res.get('std_rate', 0):.2f} B/s")

project = {
    "duration": 1,
    "nbytes_total": 1,
    "npackets_total": 1,
}
if "taxa" in selecionados:
    project["rate"] = {
        "$cond": [
            {"$gt": ["$duration", 0]},
            {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
            0
        ]
    }
if "volume" in selecionados:
    project["tipo_volume"] = {
        "$cond": [
            {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
            "Normal",
            {"$switch": {
                "branches": [
                    {"case": {"$gte": ["$nbytes_total", elefante_thresh]}, "then": "Elefante"},
                    {"case": {"$lt": ["$nbytes_total", RATO_THRESHOLD]}, "then": "Rato"},
                ],
                "default": "Normal"
            }}
        ]
    }
if "duracao" in selecionados:
    project["tipo_duracao"] = {
        "$cond": [
            {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
            "Normal",
            {"$switch": {
                "branches": [
                    {"case": {"$lt": ["$duration", LIBELULA_THRESHOLD]}, "then": "Libélula"},
                    {"case": {"$gte": ["$duration", tartaruga_thresh]}, "then": "Tartaruga"},
                ],
                "default": "Normal"
            }}
        ]
    }
if "taxa" in selecionados:
    project["tipo_taxa"] = {
        "$cond": [
            {"$lt": ["$npackets_total", MINIMUM_NPACKETS]},
            "Normal",
            {"$switch": {
                "branches": [
                    {
                        "case": 
                        {
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
                    {
                        "case": 
                        {
                            "$gte": [
                                {
                                    "$cond": [
                                        {"$gt": ["$duration", 0]},
                                        {"$divide": ["$nbytes_total", {"$divide": ["$duration", 1000]}]},
                                        0
                                    ]
                                }, chita_thresh
                            ]
                        }, "then": "Chita"},
                ],
                "default": "Normal"
            }}
        ]
    }

facet = {"total_fluxos": [{"$count": "count"}]}
if "volume" in selecionados:
    facet["contagem_volume"] = [{"$group": {"_id": "$tipo_volume", "count": {"$sum": 1}}}]
    facet["medias_volume"] = [{
        "$group": {
            "_id": "$tipo_volume",
            "media_duration": {"$avg": "$duration"},
            "media_bytes": {"$avg": "$nbytes_total"},
            "media_packets": {"$avg": "$npackets_total"},
        }
    }]
if "duracao" in selecionados:
    facet["contagem_duracao"] = [{"$group": {"_id": "$tipo_duracao", "count": {"$sum": 1}}}]
    facet["medias_duracao"] = [{
        "$group": {
            "_id": "$tipo_duracao",
            "media_duration": {"$avg": "$duration"},
            "media_bytes": {"$avg": "$nbytes_total"},
            "media_packets": {"$avg": "$npackets_total"},
        }
    }]
if "taxa" in selecionados:
    facet["contagem_taxa"] = [{"$group": {"_id": "$tipo_taxa", "count": {"$sum": 1}}}]
    facet["medias_taxa"] = [{
        "$group": {
            "_id": "$tipo_taxa",
            "media_duration": {"$avg": "$duration"},
            "media_bytes": {"$avg": "$nbytes_total"},
            "media_packets": {"$avg": "$npackets_total"},
            "media_rate": {"$avg": "$rate"},
        }
    }]

pipeline = [
    {"$project": project},
    {"$facet": facet}
]

log("Executando agregação...")
result = list(collection.aggregate(pipeline))[0]

def facet_to_df(facet_result):
    return pd.DataFrame(facet_result).rename(columns={"_id": "Categoria"}) if facet_result else pd.DataFrame(columns=["Categoria", "count"])

def get_legend_text(selecao):
    if selecao == "taxa":
        return (f"Thresholds Taxa (B/s):\n"
                f"Chita ≥ {chita_thresh:.0f}\n"
                f"Caracol < {CARACOL_RATE_THRESHOLD:.0f}")
    elif selecao == "volume":
        return (f"Thresholds Volume (bytes):\n"
                f"Elefante ≥ {elefante_thresh:.0f}\n"
                f"Rato < {RATO_THRESHOLD}")
    elif selecao == "duracao":
        return (f"Thresholds Duração (ms):\n"
                f"Tartaruga ≥ {tartaruga_thresh:.0f}\n"
                f"Libélula < {LIBELULA_THRESHOLD}")
    else:
        return ""

def plot_pie(df, title, filename, legend_text=None):
    if df.empty:
        return
    plt.figure(figsize=(9, 6))
    patches, texts, autotexts = plt.pie(df["count"], labels=df["Categoria"], autopct="%1.1f%%")
    plt.title(title)
    if legend_text:
        plt.legend(
            patches,
            df["Categoria"],
            title=legend_text,
            loc="upper left",
            bbox_to_anchor=(1.05, 1)
        )
        plt.subplots_adjust(right=0.75)
    else:
        plt.tight_layout()
    plt.savefig(os.path.join(PATH_GRAPHS, f"{today_str}_{filename}.png"))
    plt.close()

def plot_single_bar(df_means, title, filename, column, ylabel):
    if df_means.empty or column not in df_means.columns:
        return
    df_means = df_means.set_index("Categoria")
    ax = df_means[[column]].plot(kind="bar", figsize=(10,6), legend=False)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(PATH_GRAPHS, f"{today_str}_{filename}.png"))
    plt.close()

total_fluxos = result["total_fluxos"][0]["count"] if result["total_fluxos"] else 0
log(f"Total de fluxos: {total_fluxos}")

# Volume
if "volume" in selecionados:
    df_volume = facet_to_df(result["contagem_volume"])
    df_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Volume.csv"), index=False)
    plot_pie(df_volume[df_volume["Categoria"].isin(["Elefante", "Rato"])],
             f"Proporção Elefante/Rato - {NAME}", "Pie_Elefante_Rato", legend_text=get_legend_text("volume"))
    plot_pie(df_volume, f"Proporção Volume Total - {NAME}", "Pie_Volume_Todas", legend_text=get_legend_text("volume"))

    df_medias_volume = facet_to_df(result.get("medias_volume", []))
    df_medias_volume.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Volume.csv"), index=False)

    plot_single_bar(df_medias_volume, f"Média de Volume (bytes) por padrão - {NAME}", "Bar_Medias_Volume", "media_bytes", "Bytes")
    plot_single_bar(df_medias_volume, f"Média de Duração (ms) por padrão - {NAME}", "Bar_Medias_Duracao_Volume", "media_duration", "Milissegundos")
    plot_single_bar(df_medias_volume, f"Média de Pacotes por padrão - {NAME}", "Bar_Medias_Pacotes_Volume", "media_packets", "Pacotes")
    log("Geração de gráficos e CSV para volume concluída.")

# Duração
if "duracao" in selecionados:
    df_duracao = facet_to_df(result["contagem_duracao"])
    df_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Duracao.csv"), index=False)
    plot_pie(df_duracao[df_duracao["Categoria"].isin(["Libélula", "Tartaruga"])],
             f"Proporção Libélula/Tartaruga - {NAME}", "Pie_Libelula_Tartaruga", legend_text=get_legend_text("duracao"))
    plot_pie(df_duracao, f"Proporção Duração Total - {NAME}", "Pie_Duracao_Todas", legend_text=get_legend_text("duracao"))

    df_medias_duracao = facet_to_df(result.get("medias_duracao", []))
    df_medias_duracao.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Duracao.csv"), index=False)

    plot_single_bar(df_medias_duracao, f"Média de Duração (ms) por padrão - {NAME}", "Bar_Medias_Duracao", "media_duration", "Milissegundos")
    plot_single_bar(df_medias_duracao, f"Média de Volume (bytes) por padrão - {NAME}", "Bar_Medias_Volume_Duracao", "media_bytes", "Bytes")
    plot_single_bar(df_medias_duracao, f"Média de Pacotes por padrão - {NAME}", "Bar_Medias_Pacotes_Duracao", "media_packets", "Pacotes")
    log("Geração de gráficos e CSV para duração concluída.")

# Taxa
if "taxa" in selecionados:
    df_taxa = facet_to_df(result["contagem_taxa"])
    df_taxa.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Contagem_Taxa.csv"), index=False)
    plot_pie(df_taxa[df_taxa["Categoria"].isin(["Chita", "Caracol"])],
             f"Proporção Chita/Caracol - {NAME}", "Pie_Chita_Caracol", legend_text=get_legend_text("taxa"))
    plot_pie(df_taxa, f"Proporção Taxa Total - {NAME}", "Pie_Taxa_Todas", legend_text=get_legend_text("taxa"))

    df_medias_taxa = facet_to_df(result.get("medias_taxa", []))
    df_medias_taxa.to_csv(os.path.join(PATH_GRAPHS, f"{today_str}_Medias_Taxa.csv"), index=False)

    # Converter taxa para KB/s para melhor visualização
    if not df_medias_taxa.empty and "media_rate" in df_medias_taxa.columns:
        df_medias_taxa["media_rate_kbps"] = df_medias_taxa["media_rate"] / 1024
    else:
        df_medias_taxa["media_rate_kbps"] = pd.Series(dtype=float)

    plot_single_bar(df_medias_taxa, f"Média de Taxa (B/s) por padrão - {NAME}", "Bar_Medias_Taxa", "media_rate", "Bytes por segundo (B/s)")
    plot_single_bar(df_medias_taxa, f"Média de Taxa (KB/s) por padrão - {NAME}", "Bar_Medias_Taxa_KBps", "media_rate_kbps", "Kilobytes por segundo (KB/s)")
    plot_single_bar(df_medias_taxa, f"Média de Duração (ms) por padrão - {NAME}", "Bar_Medias_Duracao_Taxa", "media_duration", "Milissegundos")
    plot_single_bar(df_medias_taxa, f"Média de Pacotes por padrão - {NAME}", "Bar_Medias_Pacotes_Taxa", "media_packets", "Pacotes")
    log("Geração de gráficos e CSV para taxa concluída.")

log("Processo concluído.")
