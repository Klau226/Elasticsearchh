""" Import the connection. don't rewrite the same code every time """
from connect import es

def parse_user_query(user_query):
    """
    Μετατρέπει το Boolean query του χρήστη σε Elasticsearch DSL.
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

    # Διαχωρισμός του user_query σε όρους με βάση το AND
    conditions = user_query.split(" AND ")
    for condition in conditions:
        try:
            if "NOT" in condition:
                # Διαχείριση του NOT
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
                # Διαχείριση του OR
                sub_conditions = condition.split(" OR ")
                should_conditions = []
                for sub_condition in sub_conditions:
                    field, value = sub_condition.split(":")
                    if value.strip().upper() == "NULL":
                        should_conditions.append({"bool": {"must_not": {"exists": {"field": field.strip()}}}})
                    else:
                        should_conditions.append({"match": {field.strip(): value.strip().strip('"')}})
                query["query"]["bool"]["should"].extend(should_conditions)
            else:
                # Διαχείριση του AND
                field, value = condition.split(":")
                if ">" in value or "<" in value or "[" in value:
                    # Διαχείριση range conditions
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
                    if "[" in value and "TO" in value:
                        range_values = value.strip().strip("[]").split(" TO ")
                        range_query["gte"] = range_values[0]
                        range_query["lte"] = range_values[1]
                    query["query"]["bool"]["must"].append({"range": {field.strip(): range_query}})
                elif value.strip().upper() == "NULL":
                    # Διαχείριση NULL
                    query["query"]["bool"]["must_not"].append({"exists": {"field": field.strip()}})
                elif value.strip().upper() == "NOT NULL":
                    # Διαχείριση NOT NULL
                    query["query"]["bool"]["must"].append({"exists": {"field": field.strip()}})
                else:
                    # Απλή αντιστοίχιση
                    query["query"]["bool"]["must"].append({"match": {field.strip(): value.strip().strip('"')}})
        except ValueError:
            raise ValueError(f"Σφάλμα στο μέρος του ερωτήματος: '{condition}'. Ελέγξτε τη σύνταξή του(AND,OR,NOT και FirstName:το όνομα που θές)")
        except Exception as e:
            raise Exception(f"Σφάλμα κατά την επεξεργασία του ερωτήματος: '{condition}'. Σφάλμα: {e}")

    return query

def search_with_boolean_query(index, user_query, k = 5):
    """
    Εκτελεί την αναζήτηση στο Elasticsearch με Boolean query.
    """

    query = parse_user_query(user_query)
    try:
        response = es.search(index=index, body=query, size= k)
        hits = response['hits']['hits']

        return hits
    except Exception as e:
        print(f"Error: {e}")
        return []



def search_query_boolean_main():
    """ Wrap the functionality inside a main function so that you can call it by name """

    print("!! ΣΗΜΕΙΩΣΗ: Μπορείς να πατήσεις 'h' ή 'help' για να δεις παραδείγματα φράσεων και λέξεων !!")
    print("Δώστε το ερώτημά σας σε Boolean μορφή (π.χ., ethnicity:\"Hispanic\" AND innocence_claim:true AND age_at_arrest:[18 TO 25]):")

    while True:
        user_query = input("Ερώτημα: ").strip()
        if user_query.lower() in ["h", "help"]:
            print("\n📌 **Παραδείγματα Υποστηριζόμενων Ερωτημάτων** 📌\n")
            print("1️⃣ **Απλή Αναζήτηση**:")
            print("   - `FirstName:'το όνομα που θες'`")
            print("   - `Age:25`")
            print()
            print("2️⃣ **Σύνθετη Αναζήτηση με Λογικούς Τελεστές (Boolean Operators)**:")
            print("   - `FirstName:mario AND Age:>30`")
            print("   - `FirstName:mario AND NOT Age:<=20`")
            print("   - `Age:[10 TO 35]`")
            print()
            print("3️⃣ **Συνδυαστική Αναζήτηση με AND, OR και NOT**:")
            print("   - `FirstName:mario AND LastName:beep OR LastName:boop`")
            print("   - `FirstName:mario AND NOT LastName:baap`")
            print()
            print("4️⃣ **Αναζήτηση Πεδίων με Άδειες ή Μη Άδειες Τιμές**:")
            print("   - `FirstName:mario AND LastName:NULL`")
            print("   - `FirstName:mario AND NOT LastName:NULL`")
            print()
            print("🔹 **Σημείωση**: Μπορείς να χρησιμοποιήσεις τελεστές σύγκρισης όπως `>`, `<`, `>=`, `<=` ή εύρος `[Α TO Β]` για αριθμούς.\n")
            print("**Keywords**        ")
            print("Execution, FirstName, LastName, Age, TDCJNumber, Race, CountyOfConviction, AgeWhenReceived, EducationLevel, NativeCounty, PreviousCrime")
            print("Codefendants, NumberVictim, WhiteVictim, HispanicVictim, BlackVictim, VictimOtherRaces, FemaleVictim, MaleVictim, LastStatement\n")
        else:
            # Επεξεργασία της εισόδου του χρήστη
            print("Εκτέλεση αναζήτησης για:", user_query)
            break

    while True:
        user_input = input("Θέλεις να περιορίσεις τον αριθμό των αποτελεσμάτων; Πάτα 1 (προεπιλογή k = 5): ")
        if user_input.isdigit():  # Έλεγχος αν η είσοδος είναι αριθμητική
            user_k = int(user_input)
            if user_k == 1:
                try:
                    k = int(input("Δώστε τον αριθμό των αποτελεσμάτων (K): "))
                    print(f"Θα εμφανιστούν {k} αποτελέσματα.")
                    results = search_with_boolean_query("index_settings", user_query, k)
                except ValueError:
                    print("Μη έγκυρος αριθμός. Θα χρησιμοποιηθεί η προεπιλογή (5 αποτελέσματα).")
                    results = search_with_boolean_query("index_settings", user_query)
            else:
                print("Θα εμφανιστούν 5 αποτελέσματα (προεπιλογή).")
                results = search_with_boolean_query("index_settings", user_query)
            break  # Έξοδος από τον βρόχο όταν η είσοδος είναι έγκυρη
        else:
            print("Μη έγκυρη είσοδος! Πληκτρολογήστε έναν αριθμό.")

    try:
        # Εμφάνιση αποτελεσμάτων
        if results:
            print("Αποτελέσματα αναζήτησης:")
            for i, record in enumerate(results):
                source = record["_source"]
                score = record["_score"]
                print(f"\n🔸 Εγγραφή {i+1}: (Σκορ: {score:.2f})")
                print(f"~ {source}")

        else:
            print("Δεν βρέθηκαν αποτελέσματα για το ερώτημά σας.")
    except ValueError as ve:
        print(ve)
        exit(1)
    except Exception as e:
        print(e)
        exit(1)
