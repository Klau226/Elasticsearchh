from elasticsearch import Elasticsearch

# Σύνδεση με Elasticsearch
es = Elasticsearch("http://localhost:9200")

def delete_documents(index, document_ids):
    """
    Διαγράφει εγγραφές από το Elasticsearch.
    :param index: Το όνομα του ευρετηρίου.
    :param document_ids: Μια λίστα από IDs εγγραφών προς διαγραφή.
    """
    try:
        for doc_id in document_ids:
            es.delete(index=index, id=doc_id)
        print(f"✅ Διαγραφή {len(document_ids)} εγγραφών ολοκληρώθηκε.")
    except Exception as e:
        print(f"⚠ Σφάλμα κατά τη διαγραφή εγγραφών: {e}")

doc_ids = input("Δώστε τα IDs(Executions) των εγγραφών προς διαγραφή (χωρισμένα με κόμμα): ").strip().split(",")
delete_documents("index_settings", [doc_id.strip() for doc_id in doc_ids])