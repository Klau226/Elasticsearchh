from connect import es
import matplotlib.pyplot as plt

# Function to search and count occurrences of "god"
def search_in_last_statements(es, index, laststatement_input, category_input):
    query = {
        "query": {
            "match": {
                "LastStatement": laststatement_input
            }
        }
    }

    # Execute the query
    response = es.search(index=index, body=query, size=1000)  # Adjust size for expected number of results
    hits = response['hits']['hits']

    # Aggregate results by Race (or other relevant fields)
    counts = {}
    for hit in hits:
        category = hit["_source"].get(category_input, "Unknown")
        counts[category] = counts.get(category, 0) + 1

    return counts

# Generate bar chart from the results
def plot_distribution(counts, laststatement_input,category_input):
    plt.figure(figsize=(8, 6))

    # Sort data for consistent plotting
    sorted_counts = dict(sorted(counts.items()))
    bars = plt.bar(sorted_counts.keys(), sorted_counts.values(), color='dodgerblue')

    # Add labels to bars
    for bar in bars:
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            str(bar.get_height()),
            ha='center',
            va='bottom',
            color='black',
            fontsize=10
        )

    all_categorys = list(sorted_counts.keys())
    plt.xticks(all_categorys, fontsize=10, rotation=45)

    plt.title("Inmates Mentioning " +"'"+ laststatement_input +"'"+ " in Their Last Statements by " + category_input, fontsize=16)
    plt.xlabel(category_input, fontsize=14)
    plt.ylabel("Count", fontsize=14)
    plt.xticks(rotation=45, fontsize=12)
    plt.tight_layout()
    plt.show()

def count_inmates_for_pie(index, target_names, category_input):
    """
    Calculates the number of inmates for the data that we want to calcutate
    """
    name_counts = {}
    total_count_query = {
        "query": {
            "match_all": {}
        }
    }
    
    # Calculation of total number of inmates
    try:
        total_response = es.search(index=index, body=total_count_query, size=0)
        total_count = total_response['hits']['total']['value']
    except Exception as e:
        print(f"Σφάλμα κατά τον υπολογισμό του συνολικού αριθμού: {e}")
        return {}
    
    # Calculate number for each data
    counted_total = 0
    for name in target_names:
        query = {
            "query": {
                "match": {
                    category_input: name
                }
            }
        }
        try:
            response = es.search(index=index, body=query, size=1000)
            count = response['hits']['total']['value']
            name_counts[name] = count
            counted_total += count
        except Exception as e:
            print(f"Σφάλμα για το όνομα '{name}': {e}")
            name_counts[name] = 0
    
    # Calculate Others
    name_counts["Others"] = total_count - counted_total
    return name_counts

def make_pie(index, target_names, category_input):
    """
    Making pie chart
    """
    # Frequency calculation for each name and Others
    name_counts = count_inmates_for_pie(index, target_names, category_input)
    
    # Data for the chart
    names = list(name_counts.keys())
    counts = list(name_counts.values())
    
    # Creates a pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(counts, labels=names, autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
    plt.title("Distribution of Inmates by " + category_input , fontsize=16)
    plt.tight_layout()
    plt.show()


def statistics():

    print("Θέλεις να δείς στατιστικά σε μορφή γραφήματος ή σε μορφή 'πίτας'; Σημείωση το γράφημα ψάχνει λέξεις/φράσης από το LastStatement")
    choice = input("Για γράφημα πάτα 1 αλλιώς για 'πίτα' πάτα 2: ")

    index = "index_settings" 
    while(True):
        if choice == '1':
            laststatement_input = input("Τι θέλεις να ψάξεις στο LastStatement; \n")
            category_input = input("Και σε ποιά κατηγορία θέλεις να ψάξεις το " +"'"+ laststatement_input +"'"+ "; ")
            race_counts = search_in_last_statements(es, index, laststatement_input, category_input)
            plot_distribution(race_counts ,laststatement_input ,category_input)
            break
        elif choice == '2':
            category_input = input("Σε ποιά κατηγορία θέλεις να ψάξεις; ")
            user_input = input("Δώστε τα ονόματα που θέλετε να αναζητήσετε ή και τι τιμές, χωρισμένα με κόμμα (π.χ. George,John,Michael): ").strip()
            names = [name.strip() for name in user_input.split(",") if name.strip()]
            make_pie(index, names, category_input)
            break
        else:
            print("Πρέπει να πατήσετε είτε 1 είτε 2")
    