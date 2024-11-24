# Elasticsearch Inmate Search Script

This script allows you to search for inmates in an Elasticsearch index using first and last names as search criteria.

## Prerequisites

- Python 3.x
- Elasticsearch running locally on `http://localhost:9200`
- Required Python packages listed in `requirements.txt`

## Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd <repository-directory>
```

## Set up a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

## Install required packages:
Run:
```bash
pip install -r requirements.txt
```

## Usage
Run the script with the desired search criteria. You can search by first name, last name, or both.
```bash
python search.py --first_name=John
python search.py --last_name=Doe
python search.py --first_name=John --last_name=Doe
```

Notes
Ensure your Elasticsearch instance is running and accessible at http://localhost:9200.
Modify the index name (index_settings) in the script if your index is named differently.
