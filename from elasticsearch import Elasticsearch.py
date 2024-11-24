from elasticsearch import Elasticsearch
import csv

es = Elasticsearch(["hsttp://localhost:9200"])

index_settings = {
    "settings": {
        "analysis": {
            "filter": {
                "my_stop": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "porter_stemmer": {
                    "type": "porter_stem"
                }
            },
            "analyzer": {
                "my_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "my_stop", "porter_stemmer"]
                }
            },
            "normalizer": {
                "lowercase_normalizer": {
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "Execution": {"type": "integer"},
            "LastName": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "raw": {"type": "keyword"}
                }
            },
            "Execution": {"type": "integer"},
            "FirstName": {"type": "text", "analyzer": "standard"},
            "TDCJNumber": {"type": "keyword"},
            "Age": {"type": "integer"},
            "Race": {"type": "keyword", "normalizer": "lowercase_normalizer"},
            "CountyOfConviction": {"type": "text", "analyzer": "standard"},
            "AgeWhenReceived": {"type": "integer"},
            "EducationLevel": {"type": "integer"},
            "NativeCounty ": {"type": "integer"},
            "PreviousCrime": {"type": "integer"},
            "Codefendants": {"type": "integer"},
            "NumberVictim": {"type": "integer"},
            "WhiteVictim": {"type": "integer"},
            "HispanicVictim": {"type": "integer"},
            "BlackVictim": {"type": "integer"},
            "VictimOther Races": {"type": "integer"},
            "FemaleVictim": {"type": "integer"},
            "MaleVictim": {"type": "integer"},
            "LastStatement": {"type": "text", "analyzer": "my_analyzer"}
        }
    }
}

# Create the index
es.indices.create(index="index_settings", body=index_settings)

# Ingest the data
with open('Texas Last Statement_utf8.csv', 'r', encoding='UTF-8') as csvfile:
    reader = csv.DictReader(csvfile)
    #reader.fieldnames = [name.strip() for name in reader.fieldnames] Doesnt work
    #reader.fieldnames = [name.strip().replace(' ', '_').replace('\u00a0', '').replace('\t', '') 
                         #for name in reader.fieldnames]

    #print("Cleaned fieldnames:", reader.fieldnames)
    row_count = 0
    for row in reader:
        try:
            row_count += 1
            # Prepare processed fields
            doc = {
                "Execution": int(row["Execution"]) if row["Execution"] and row["Execution"].strip() != 'NA' else None,
                "FirstName": row["FirstName"].strip() if row["FirstName"] and row["FirstName"].strip() != 'NA' else None,
                "LastName": row["LastName"].strip() if row["LastName"] and row["LastName"].strip() != 'NA' else None,
                "TDCJNumber": int(row["TDCJNumber"]) if row["TDCJNumber"] and row["TDCJNumber"].strip() != 'NA' else None,
                "Age": int(row["Age"]) if row["Age"] and row["Age"].strip() != 'NA' else None,
                "Race": row["Race"].strip() if row["Race"] and row["Race"].strip() != 'NA' else None,
                "CountyOfConviction": row["CountyOfConviction"].strip() if row["CountyOfConviction"] and row["CountyOfConviction"].strip() != 'NA' else None,
                "AgeWhenReceived": int(row["AgeWhenReceived"]) if row["AgeWhenReceived"] and row["AgeWhenReceived"].strip() != 'NA' else None,
                "EducationLevel": int(row["EducationLevel"]) if row["EducationLevel"] and row["EducationLevel"].strip() != 'NA' else None,
                "NativeCounty": int(row["NativeCounty "]) if row["NativeCounty "] and row["NativeCounty "].strip() != 'NA' else None,
                #"NativeCounty ": bool(int(row["NativeCounty "])) if row["NativeCounty "] and row["NativeCounty "].strip() != 'NA' else None,
                "PreviousCrime": int(row["PreviousCrime"]) if row["PreviousCrime"] and row["PreviousCrime"].strip() != 'NA' else None,
                "Codefendants": int(row["Codefendants"]) if row["Codefendants"] and row["Codefendants"].strip() != 'NA' else None,
                "NumberVictim": int(row["NumberVictim"]) if row["NumberVictim"] and row["NumberVictim"].strip() != 'NA' else None,
                "WhiteVictim": int(row["WhiteVictim"]) if row["WhiteVictim"] and row["WhiteVictim"].strip() != 'NA' else None,
                "HispanicVictim": int(row["HispanicVictim"]) if row["HispanicVictim"] and row["HispanicVictim"].strip() != 'NA' else None,
                "BlackVictim": int(row["BlackVictim"]) if row["BlackVictim"] and row["BlackVictim"].strip() != 'NA' else None,
                "VictimOtherRaces": int(row["VictimOther Races"]) if row["VictimOther Races"] and row["VictimOther Races"].strip() != 'NA' else None,
                "FemaleVictim": int(row["FemaleVictim"]) if row["FemaleVictim"] and row["FemaleVictim"].strip() != 'NA' else None,
                "MaleVictim": int(row["MaleVictim"]) if row["MaleVictim"] and row["MaleVictim"].strip() != 'NA' else None,
                "LastStatement": row["LastStatement"].strip() if row["LastStatement"] and row["LastStatement"].strip() != 'NA' else None
            }

            print("Indexed document:", doc)  # Optional for debugging

            # Index document
            es.index(index="index_settings", body=doc)

        except Exception as e:
            print(f"Error processing row {row_count}: {e}")
            #print("Row data:", row)

    print(f"Total rows processed: {row_count}")
    #print("Indexed document:", doc)  # Optional for debugging

