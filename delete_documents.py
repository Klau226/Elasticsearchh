from connect import es

def delete_documents(index, document_ids):
    """
    Deletes records from Elasticsearch by Execution ID.
    """
    try:
        for doc_id in document_ids:
            es.delete(index=index, id=doc_id)
        print(f"✅ Διαγραφή {len(document_ids)} εγγραφών ολοκληρώθηκε.")
    except Exception as e:
        print(f"⚠ Σφάλμα κατά τη διαγραφή εγγραφών: {e}")

def delete_documents_main():
    doc_ids = input("Δώστε τα IDs(Executions) των εγγραφών προς διαγραφή (χωρισμένα με κόμμα): ").strip().split(",")
    delete_documents("index_settings", [doc_id.strip() for doc_id in doc_ids])