import pymongo
import time

# Aqui faz a conexão com o banco de dados
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

# Data base name
DATA_BASE_NAME = "fluxos_database"

# Collection original name
COLLECTION_NAME = "caida_collection"
# Collection copia name
COLLECTION_NAME_COPY = "caida_collection_copy"

start = time.time()

# Faz a copia da collection original para a collection copia
mongo_client[DATA_BASE_NAME][COLLECTION_NAME].aggregate([
    {"$out": COLLECTION_NAME_COPY}
])

# Fecha a conexão com o banco de dados
mongo_client.close()

# Tempo de execução 
print("Tempo de execução:", time.time() - start)
