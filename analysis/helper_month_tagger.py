from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm

client = MongoClient()  # Connect to MongoDB

# Hyperparameters
DB_NAME = 'reddit'  # Your database name
COMMENTS_COLLECTION_NAME = 'relevant_comments'  # Your comments collection name
SUBMISSIONS_COLLECTION_NAME = 'relevant_submissions'  # Your submissions collection name

db = client[DB_NAME]  # Connect to the database

def add_month_field(collection_name):
    collection = db[collection_name]
    for doc in tqdm(collection.find({"month": {"$exists": False}}), desc=f"Updating {collection_name}"):
        try:
            # Convert 'created_utc' to integer if it's a string
            created_utc = int(doc["created_utc"])
        except ValueError:
            # Handle documents where 'created_utc' may not be a valid integer
            print(f"Invalid 'created_utc' in document ID {doc['_id']}")
            continue  # Skip this document

        month = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m')
        collection.update_one({"_id": doc["_id"]}, {"$set": {"month": month}})

def main():
    add_month_field(COMMENTS_COLLECTION_NAME)
    add_month_field(SUBMISSIONS_COLLECTION_NAME)
    print("Month tagging completed.")

if __name__ == "__main__":
    main()
