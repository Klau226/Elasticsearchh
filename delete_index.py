from connect import es

# Delete all records from Elastic Search
def delete_index_main():
    es.indices.delete(index="index_settings", ignore=[400, 404])
    print("Index deleted successfully.")