from connect import es

def get_next_execution_id(index):
    """
    Calculates the next Execution ID.
    """
    try:
        response = es.search(index=index, body={
            "size": 0, 
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
        return 1  

def check_and_update_by_name(index, record):
    """
    Checks if there is a first name and a last name.
    If there is, updates the record,
    if there is not, makes a new record with a new Execution ID.
    """
    first_name = record.get("FirstName", "NA")
    last_name = record.get("LastName", "NA")
    
    try:
        # Searching for the exact first name and last name
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
            # If there is one, update it
            existing_doc_id = response["hits"]["hits"][0]["_id"]
            es.index(index=index, id=existing_doc_id, document=record)
            print(f"🔄 Υπάρχει ήδη εγγραφή με FirstName '{first_name}' και LastName '{last_name}'. Ενημερώθηκε η εγγραφή.")
        else:
            # If none, make a new document
            new_execution = get_next_execution_id(index)
            record["Execution"] = new_execution
            es.index(index=index, id=new_execution, document=record)
            print(f"✅ Δεν βρέθηκε εγγραφή με FirstName '{first_name}' και LastName '{last_name}'. Δημιουργήθηκε νέα εγγραφή με Execution {new_execution}.")
    except Exception as e:
        print(f"⚠ Σφάλμα κατά την ενημέρωση/προσθήκη εγγραφής: {e}")

def get_user_input(default_record):
    """
    User input for the new/updated document
    """
    user_record = {}
    print("Πληκτρολογήστε τις τιμές για κάθε πεδίο. Πατήστε Enter για χρήση της προεπιλεγμένης τιμής (NA ή NULL).")
    
    for field, default_value in default_record.items():
        user_input = input(f"{field} (Προεπιλογή: {default_value}): ").strip()
        if user_input:
            # Convert values ​​to numbers if needed
            if default_value is None:  # If the field requires a numeric value
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

def add_document_main():
    # Record that has default inputs if the user doesn't want to insert anything
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

    # Get data from the user
    print("Συμπληρώστε τα πεδία για την εγγραφή:")
    new_record = get_user_input(default_record)

    # Update or add a record
    check_and_update_by_name("index_settings", new_record)