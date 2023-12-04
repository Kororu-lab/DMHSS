from pymongo import MongoClient
from tqdm import tqdm
import logging

# Setup logging
logging.basicConfig(filename='migration_log.log', level=logging.INFO, format='%(asctime)s: %(message)s')

# Hyperparameters
DB_NAME = 'reddit'
COLLECTION_NAME_FROM = 'filtered_comments_sentiment'  # Change as needed
COLLECTION_NAME_TO = 'filtered_comments_standard'  # Change as needed
COLLECTION_TYPE = 'C'  # 'S' for submissions or 'C' for comments

# MongoDB setup
client = MongoClient()
db = client[DB_NAME]

def transfer_sentiment_scores(collection_name_from, collection_name_to, collection_type):
    collection_from = db[collection_name_from]
    collection_to = db[collection_name_to]

    for doc in tqdm(collection_from.find(), desc="Processing documents"):
        query = {
            'created_utc': doc['created_utc'],
            'author': doc['author']
        }

        # Construct update object based on collection type
        if collection_type == 'S':  # Submissions
            update = {
                '$set': {
                    'sentiment_score_complex': doc.get('sentiment_score_complex', None),
                    'sentiment_score_text': doc.get('sentiment_score_text', None),
                    'sentiment_score_title': doc.get('sentiment_score_title', None)
                }
            }
        elif collection_type == 'C':  # Comments
            update = {
                '$set': {
                    'sentiment_score': doc.get('sentiment_score', None)
                }
            }
        else:
            raise ValueError("Invalid collection type. Use 'S' for submissions or 'C' for comments.")

        # Update the document in the target collection
        matches = collection_to.count_documents(query)
        if matches == 1:
            collection_to.update_one(query, update)
        elif matches > 1:
            logging.warning(f"Multiple matches found for document with _id: {doc['_id']}")
        else:
            logging.info(f"No match found for document with _id: {doc['_id']}")

# Execute the function
transfer_sentiment_scores(COLLECTION_NAME_FROM, COLLECTION_NAME_TO, COLLECTION_TYPE)
