""" Import functions from different files """
from search_query_boolean import search_query_boolean_main
from add_document import add_document_main
from delete_documents import delete_documents_main
from populate import populate_main
from delete_index import delete_index_main
from inmate_statistics import statistics

def main() -> None:
    """ Main scipt where the user chooses what he wants to do """

    print("\n")
    print("!! ΣΗΜΕΙΩΣΗ: Πληκτρολογήστε 8 εάν είναι η πρώτη φόρα που εκκινείται το πρόγραμμα για την προσθήκη εγγράφων, 9 για διαγραφή όλων των εγγράφων\n")
    while True:
            # Prompt the user for input
            
            user_input = input("Πληκτρολογήστε 1 για αναζήτηση, 2 προσθήκη εγγράφων, 3 για διαγραφή εγγράφων, 4 για στατιστικά κρατουμένων ή 'exit' για έξοδο: \n")
            
            if user_input == '1':
                search_query_boolean_main()  # Search Documents
            elif user_input == '2':          
                add_document_main()          # Add Documents
            elif user_input == '3':
                delete_documents_main()      # Delete Documents
            elif user_input == '4':
                statistics()                 # Statistics
            elif user_input == '8':
                populate_main()                # Populate Elastic Search
            elif user_input == '9':
                delete_index_main()          # Delete All Documents From Elastic Search
            elif user_input.lower() == 'exit':
                print("Έξοδος από το πρόγραμμα.")
                break  # Exit the loop and end the program
            else:
                print("⚠️  Μη έγκυρη επιλογή. Παρακαλώ δοκιμάστε ξανά.\n")


if __name__ == "__main__":
    main()