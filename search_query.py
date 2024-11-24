from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

# Λειτουργία αναζήτησης
def search_records(index, query):
    try:
        response = es.search(index=index, body=query)
        return response['hits']['hits']
    except Exception as e:
        print(f"Error: {e}")
        return []

def build_query(criteria):
    bool_query = {"bool": {"must": [], "must_not": [], "should": []}}

    # Παράδειγμα: Boolean ερώτημα
    if "FirstName" in criteria:
        bool_query["bool"]["must"].append({"match": {"FirstName": criteria["FirstName"]}})
    if "LastName" in criteria:
        bool_query["bool"]["must"].append({"match": {"LastName": criteria["LastName"]}})
    """if "innocence_claim" in criteria and criteria["innocence_claim"]:
        bool_query["bool"]["must"].append({"match": {"last_statement": "innocent"}})
    if "min_age" in criteria or "max_age" in criteria:
        range_query = {}
        if "min_age" in criteria:
            range_query["gte"] = criteria["min_age"]
        if "max_age" in criteria:
            range_query["lte"] = criteria["max_age"]
        bool_query["bool"]["must"].append({"range": {"age_at_arrest": range_query}})
    if "no_last_statement" in criteria and criteria["no_last_statement"]:
        bool_query["bool"]["must_not"].append({"exists": {"field": "last_statement"}})"""

    return {"query": bool_query} 

criteria = {
        "FirstName": "george",
        "LastName": "rivas"
    }

# Δημιουργία ερωτήματος
query = build_query(criteria)
    
# Αναζήτηση στο index "texas_inmates"
results = search_records("index_settings", query)
    
# Εμφάνιση αποτελεσμάτων
for record in results:
    print(record["_source"])