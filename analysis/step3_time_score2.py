import pymongo
import pandas as pd

# MongoDB Connection Parameters
DB_NAME = 'reddit'
COLLECTION_NAMES = ['filtered_comments_standard', 'filtered_comments_standard']  # Add your collection names
BATCH_SIZE = 500  # Adjustable batch size for processing large datasets

client = pymongo.MongoClient()
db = client[DB_NAME]

def calculate_communication_scores(db, collection_name):
    pipeline = [
        # Add your aggregation stages here
    ]
    return list(db[collection_name].aggregate(pipeline, batchSize=BATCH_SIZE))

def update_document_scores(db, collection_name, scores):
    collection = db[collection_name]
    for score in scores:
        query = {"author": score['author'], "created_day": {"$regex": score['month']}}
        update = {"$set": {"comm_score": score['normalized_score']}}
        collection.update_many(query, update)

def main():
    for collection_name in COLLECTION_NAMES:
        print(f"Calculating scores for {collection_name}...")
        scores = calculate_communication_scores(db, collection_name)
        print(f"Updating documents in {collection_name}...")
        update_document_scores(db, collection_name, scores)

if __name__ == "__main__":
    main()
