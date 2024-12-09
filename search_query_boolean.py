from connect import es

def parse_user_query(user_query):
    """
    Converts the user's Boolean query to an Elasticsearch DSL.
    """
    query = {
        "query": {
            "bool": {
                "must": [],
                "must_not": [],
                "should": []
            }
        }
    }

    # Split user_query into conditions based on AND or OR
    conditions = user_query.split(" AND ")
    for condition in conditions:
        try:
            if "NOT" in condition:
                # Management of NOT
                field, value = condition.replace("NOT ", "").split(":")
                if "<" in value or ">" in value:
                    range_query = {}
                    if "<" in value:
                        range_query["lt"] = value.split("<")[1].strip()
                    if ">" in value:
                        range_query["gt"] = value.split(">")[1].strip()
                    query["query"]["bool"]["must_not"].append({"range": {field.strip(): range_query}})
                else:
                    query["query"]["bool"]["must_not"].append({"match": {field.strip(): value.strip().strip('"')}})
            elif "OR" in condition:
                # Management of OR
                sub_conditions = condition.split(" OR ")
                should_conditions = []
                for sub_condition in sub_conditions:
                    field, value = sub_condition.split(":")
                    if ">" in value or "<" in value or "[" in value:
                        # Handle range conditions in OR
                        range_query = {}
                        if ">" in value:
                            if ">=" in value:
                                range_query["gte"] = value.split(">=")[1].strip()
                            else:
                                range_query["gt"] = value.split(">")[1].strip()
                        if "<" in value:
                            if "<=" in value:
                                range_query["lte"] = value.split("<=")[1].strip()
                            else:
                                range_query["lt"] = value.split("<")[1].strip()
                        if "[" in value and "TO" in value:  # From ex.[10 TO 20] or [30 TO 70]
                            range_values = value.strip().strip("[]").split(" TO ")
                            range_query["gte"] = range_values[0].strip()
                            range_query["lte"] = range_values[1].strip()
                        should_conditions.append({"range": {field.strip(): range_query}})
                    elif value.strip().upper() == "NULL":
                        # Management of NULL in OR
                        should_conditions.append({"bool": {"must_not": {"exists": {"field": field.strip()}}}})
                    else:
                        # Simple matching in OR
                        should_conditions.append({"match": {field.strip(): value.strip().strip('"')}})
                query["query"]["bool"]["should"].extend(should_conditions)
            else:
                # Management of AND
                field, value = condition.split(":")
                if ">" in value or "<" in value or "[" in value:
                    # Range conditions (for >,>=,<,<=)
                    range_query = {}
                    if ">" in value:
                        if ">=" in value:
                            range_query["gte"] = value.split(">=")[1].strip()
                        else:
                            range_query["gt"] = value.split(">")[1].strip()
                    if "<" in value:
                        if "<=" in value:
                            range_query["lte"] = value.split("<=")[1].strip()
                        else:
                            range_query["lt"] = value.split("<")[1].strip()
                    if "[" in value and "TO" in value: # From ex.[10 TO 20] or [30 TO 70]
                        range_values = value.strip().strip("[]").split(" TO ")
                        range_query["gte"] = range_values[0]
                        range_query["lte"] = range_values[1]
                    query["query"]["bool"]["must"].append({"range": {field.strip(): range_query}})
                elif value.strip().upper() == "NULL":
                # NULL logic
                    query["query"]["bool"]["must_not"].append({"exists": {"field": field.strip()}})
                else:
                    # Simple matching
                    query["query"]["bool"]["must"].append({"match": {field.strip(): value.strip().strip('"')}})
        except ValueError:
            raise ValueError(f"Σφάλμα στο μέρος του ερωτήματος: '{condition}'. Ελέγξτε τη σύνταξή του(AND,OR,NOT και π.χ(FirstName:το όνομα που θές) ή Age:>13) ΣΗΜΕΙΩΣΗ: Μπορείς πάντα να πατήσεις 'h' για βοήθεια")
        except Exception as e:
            raise Exception(f"Σφάλμα κατά την επεξεργασία του ερωτήματος: '{condition}'. Σφάλμα: {e}")

    return query

def search_with_boolean_query(index, user_query, k = 5):
    """
    Performs the search in Elasticsearch with a Boolean query.
    """
    
    try:
        query = parse_user_query(user_query) 
        response = es.search(index=index, body=query, size=k)
        hits = response['hits']['hits']
        return hits
    except ValueError as ve:
        print(f"🔴 Σφάλμα σύνταξης: {ve}")
        return []
    except Exception as e:
        print(f"🔴 Σφάλμα κατά την αναζήτηση: {e}")
        return []

def search_query_boolean_main():
    print("!! ΣΗΜΕΙΩΣΗ: Μπορείς να πατήσεις 'h' ή 'help' για να δεις παραδείγματα φράσεων και λέξεων !!")
    print("Δώστε το ερώτημά σας σε Boolean μορφή (π.χ., ethnicity:\"Hispanic\" AND innocence_claim:true AND age_at_arrest:[18 TO 25]):")

    while True:
        user_query = input("Ερώτημα: ").strip()
        # Instructions
        if user_query.lower() in ["h", "help"]:
            print("\n📌 **Παραδείγματα Υποστηριζόμενων Ερωτημάτων** 📌\n")
            print("1️⃣ **Απλή Αναζήτηση**:")
            print("   - `FirstName:'το όνομα που θες'`")
            print("   - `Age:25`")
            print()
            print("2️⃣ **Σύνθετη Αναζήτηση με Λογικούς Τελεστές (Boolean Operators)**:")
            print("   - `FirstName:John AND Age:>30`")
            print("   - `FirstName:John AND NOT Age:<=20`")
            print("   - `Age:[10 TO 35]`")
            print()
            print("3️⃣ **Συνδυαστική Αναζήτηση με AND, OR και NOT**:")
            print("   - `FirstName:John AND LastName:beep OR LastName:boop`")
            print("   - `FirstName:John AND NOT LastName:baap`")
            print()
            print("4️⃣ **Αναζήτηση Πεδίων με Άδειες ή Μη Άδειες Τιμές**:")
            print("   - `FirstName:john AND LastName:NULL`")
            print("   - `FirstName:john AND NOT LastName:NULL`")
            print()
            print("🔹 **Σημείωση**: Μπορείς να χρησιμοποιήσεις τελεστές σύγκρισης όπως `>`, `<`, `>=`, `<=` ή εύρος `[Α TO Β]` για αριθμούς.\n")
            print("**Keywords**        ")
            print("Execution, FirstName, LastName, Age, TDCJNumber, Race, CountyOfConviction, AgeWhenReceived, EducationLevel, NativeCounty, PreviousCrime")
            print("Codefendants, NumberVictim, WhiteVictim, HispanicVictim, BlackVictim, VictimOtherRaces, FemaleVictim, MaleVictim, LastStatement\n")
        else:
            # Edit user input
            print("Εκτέλεση αναζήτησης για:", user_query)
            break

    while True:
        try:
            user_input = input("Θέλεις να περιορίσεις τον αριθμό των αποτελεσμάτων; Πάτα 1 (προεπιλογή k = 5): \n") # checks if the user wants Top K- results
            if user_input.isdigit():
                user_k = int(user_input)
                if user_k == 1:
                    k = int(input("Δώστε τον αριθμό των αποτελεσμάτων (K): ").strip())
                else:
                    k = 5  # Default
                break
            else:
                print("🔴 Μη έγκυρη είσοδος! Πληκτρολογήστε έναν αριθμό.")
        except ValueError:
             print("🔴 Μη έγκυρη είσοδος! Προσπαθήστε ξανά.")

    results = search_with_boolean_query("index_settings", user_query, k)
    try:
        # Show results
        if results:
            print("Αποτελέσματα αναζήτησης:")
            for i, record in enumerate(results):   
                source = record["_source"]
                score = record["_score"]
                print(f"\n🔸 Εγγραφή {i+1}: (Σκορ: {score:.2f})")
                print(f"~ {source}")
                    
        else:
            print("⛔ Δεν βρέθηκαν αποτελέσματα για το ερώτημά σας.\n")
    except ValueError as ve:  # try-except for errors that i didnt encounter in my testings with the _source and _score
        print(ve)
        exit(1)
    except Exception as e:
        print(e)
        exit(1)