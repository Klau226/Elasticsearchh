from elasticsearch import Elasticsearch
import argparse


"""Run the script with the desired search criteria
Example
python search.py --first_name=John
python search.py --last_name=Doe
python search.py --first_name=John --last_name=Doe"""

es = Elasticsearch(["http://localhost:9200"])

def search_records(index, query):
    try:
        response = es.search(index=index, body=query)
        return response['hits']['hits']
    except Exception as e:
        print(f"Error: {e}")
        return []

def build_query(criteria):
    bool_query = {"bool": {"must": []}}

    if "first_name" in criteria:
        bool_query["bool"]["must"].append({"match": {"FirstName": criteria["first_name"]}})
    if "last_name" in criteria:
        bool_query["bool"]["must"].append({"match": {"LastName": criteria["last_name"]}})

    #... Rest of the search criteria

    return {"query": bool_query}

def search(criteria):
    query = build_query(criteria)
    return search_records("index_settings", query)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for Texas inmates")
    parser.add_argument("--first_name", help="First name of the inmate")
    parser.add_argument("--last_name", help="Last name of the inmate")
    args = parser.parse_args()

    criteria = {}
    if args.first_name:
        criteria["first_name"] = args.first_name
    if args.last_name:
        criteria["last_name"] = args.last_name

    if criteria:
        print(f"Searching for {args.first_name or ''} {args.last_name or ''}")
        results = search(criteria)

        if not results:
            print("- No records found")

        for record in results:
            print(f"- {record['_source']['FirstName']} {record['_source']['LastName']}")
    else:
        print("Please provide at least one search criteria (--first_name or --last_name)")

