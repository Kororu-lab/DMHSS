from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm

client = MongoClient()  # Connect to MongoDB

# Hyperparameters
DB_NAME = 'reddit'  # Your database name
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'  # Your comments collection name
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'  # Your submissions collection name

db = client[DB_NAME]  # Connect to the database

def add_month_field(collection_name):
    collection = db[collection_name]
    for doc in tqdm(collection.find({}), desc=f"Updating {collection_name}"):
        month = datetime.utcfromtimestamp(doc["created_utc"]).strftime('%Y-%m')
        collection.update_one({"_id": doc["_id"]}, {"$set": {"month": month}})

def main():
    add_month_field(COMMENTS_COLLECTION_NAME)
    add_month_field(SUBMISSIONS_COLLECTION_NAME)
    print("Month tagging completed.")

if __name__ == "__main__":
    main()
