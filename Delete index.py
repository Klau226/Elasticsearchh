from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

# Delete the index
es.indices.delete(index="index_settings", ignore=[400, 404])

print("Index deleted successfully.")