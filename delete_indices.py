from connect import es

def delete_indices():
    # Delete the index
    es.indices.delete(index="index_settings", ignore=[400, 404])

    print("Index deleted successfully.")
