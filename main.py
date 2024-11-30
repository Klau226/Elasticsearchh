import argparse
""" Import functions from different files """
from search_query_boolean import search_query_boolean_main
from delete_indices import delete_indices
from populate import populate_main

""" Execute with the desired arguments
    Examples:
    - python main.py --search_query_boolean
    - python main.py --delete_indiced
    - python main.py --populate_main
    .... Add more arguments accordingly..

"""

def main() -> None:

    """ Read arguments from the command line """
    parser = argparse.ArgumentParser(
        description="A tool to read and evaluate Fotokite's Code Category configuration."
    )
    parser.add_argument(
        "--search_query_boolean",
        action="store_true",
        help="",
    )
    parser.add_argument(
        "--delete_indices",
        action="store_true",
        help="",
    )
    parser.add_argument(
        "--populate_main",
        action="store_true",
        help="",
    )

    """ Parse the arguments to a variable """
    args = parser.parse_args()

    """ Depending on the argument, call the appropriate function """
    if args.search_query_boolean:
        search_query_boolean_main()

    if args.delete_indices:
        delete_indices()

    if args.populate_main:
        populate_main()


if __name__ == "__main__":
    main()
