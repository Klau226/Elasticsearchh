from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

def get_next_execution_id(index):
    """
    Υπολογίζει το επόμενο διαθέσιμο Execution ID από το ευρετήριο.
    """
    try:
        response = es.search(index=index, body={
            "size": 0,  # Δεν χρειάζονται αποτελέσματα
            "aggs": {
                "max_execution": {
                    "max": {
                        "field": "Execution"
                    }
                }
            }
        })
        max_execution = response['aggregations']['max_execution']['value']
        return int(max_execution) + 1 if max_execution else 1
    except Exception as e:
        print(f"⚠ Σφάλμα κατά την εύρεση του επόμενου Execution ID: {e}")
        return 1  # Αν το ευρετήριο είναι κενό

def check_and_update_by_name(index, record):
    """
    Ελέγχει αν υπάρχει ήδη εγγραφή με FirstName και LastName.
    Αν υπάρχει, την ενημερώνει. Αν όχι, δημιουργεί νέα εγγραφή.
    """
    first_name = record.get("FirstName", "NA")
    last_name = record.get("LastName", "NA")
    
    try:
        # Αναζητήστε εγγραφή με το ίδιο FirstName και LastName
        response = es.search(index=index, body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"FirstName": first_name}},
                        {"match": {"LastName": last_name}}
                    ]
                }
            }
        })

        if response["hits"]["total"]["value"] > 0:
            # Αν υπάρχει εγγραφή, ενημερώστε την
            existing_doc_id = response["hits"]["hits"][0]["_id"]
            #es.update(index=index, id=existing_doc_id, doc={"doc": record})
            es.index(index=index, id=existing_doc_id, document=record)
            print(f"🔄 Υπάρχει ήδη εγγραφή με FirstName '{first_name}' και LastName '{last_name}'. Ενημερώθηκε η εγγραφή.")
        else:
            # Αν δεν υπάρχει, δημιουργήστε νέα εγγραφή
            new_execution = get_next_execution_id(index)
            record["Execution"] = new_execution
            es.index(index=index, id=new_execution, document=record)
            print(f"✅ Δεν βρέθηκε εγγραφή με FirstName '{first_name}' και LastName '{last_name}'. Δημιουργήθηκε νέα εγγραφή με Execution {new_execution}.")
    except Exception as e:
        print(f"⚠ Σφάλμα κατά την ενημέρωση/προσθήκη εγγραφής: {e}")

def get_user_input(default_record):
    """
    Λαμβάνει είσοδο από τον χρήστη και επιστρέφει ένα συμπληρωμένο αρχείο εγγραφής.
    """
    user_record = {}
    print("Πληκτρολογήστε τις τιμές για κάθε πεδίο. Πατήστε Enter για χρήση της προεπιλεγμένης τιμής (NA ή NULL).")
    
    for field, default_value in default_record.items():
        user_input = input(f"{field} (Προεπιλογή: {default_value}): ").strip()
        if user_input:
            # Μετατροπή τιμών σε αριθμούς, αν χρειάζεται
            if default_value is None:  # Αν το πεδίο απαιτεί αριθμητική τιμή
                try:
                    user_record[field] = int(user_input)
                except ValueError:
                    print(f"⚠ Το πεδίο {field} πρέπει να είναι αριθμός. Χρησιμοποιείται η προεπιλεγμένη τιμή.")
                    user_record[field] = default_value
            else:
                user_record[field] = user_input
        else:
            user_record[field] = default_value
    
    return user_record
# Παράδειγμα προσθήκης νέας εγγραφής
default_record = {
    "LastName": "NA",
    "FirstName": "NA",
    "TDCJNumber": "NA",
    "Age": None,
    "Race": "NA",
    "CountyOfConviction": "NA",
    "AgeWhenReceived": None,
    "EducationLevel": None,
    "NativeCounty": None,
    "PreviousCrime": None,
    "Codefendants": None,
    "NumberVictim": None,
    "WhiteVictim": None,
    "HispanicVictim": None,
    "BlackVictim": None,
    "VictimOther Races": None,
    "FemaleVictim": None,
    "MaleVictim": None,
    "LastStatement": "NA"
}

# Λήψη δεδομένων από τον χρήστη
print("Συμπληρώστε τα πεδία για την εγγραφή:")
new_record = get_user_input(default_record)

# Ενημέρωση ή προσθήκη εγγραφής
check_and_update_by_name("index_settings", new_record)