from elasticsearch import Elasticsearch
import argparse
import matplotlib.pyplot as plt  # Add this line



"""Run the script with the desired search criteria
Example
python search.py --first_name=John
python search.py --last_name=Doe
python search.py --first_name=John --last_name=Doe"""

es = Elasticsearch(["http://localhost:9200"])

def search_records(index, query):
    try:
        response = es.search(index=index, body=query, size=10000)
        return response['hits']['hits']
    except Exception as e:
        print(f"Error: {e}")
        return []

def build_query(criteria):
    bool_query = {"bool": {"must": []}}

    for criteria_key, criteria_value in criteria.items():
        if criteria_value:
            bool_query["bool"]["must"].append({"match": {criteria_key: criteria [criteria_key]}})

    print(bool_query)
    return {"query": bool_query}

def search(criteria):
    query = build_query(criteria)
    return search_records("index_settings", query)

# Create a bar chart that shows the number
def bar_chart_by_race(hispanics, blacks, whites):
    races = ['Hispanic', 'Black', 'White']
    counts = [len(hispanics), len(blacks), len(whites)]

    plt.figure(figsize=(10, 6))
    plt.bar(races, counts)
    plt.title('Number of Inmates by Race')
    plt.xlabel('Race')
    plt.ylabel('Number of Inmates')

    # Add the count on top of each bar
    for i, count in enumerate(counts):
        plt.text(i, count, str(count), ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig('inmates_by_race.png')
    plt.close()

    print(f"Bar chart saved as 'inmates_by_race.png'")

if __name__ == "__main__":
    hispanics = search({"Race": "Hispanic"})
    blacks = search({"Race": "Black"})
    whites = search({"Race": "White"})
    bar_chart_by_race(hispanics, blacks, whites)
