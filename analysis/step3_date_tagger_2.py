import pymongo
from datetime import datetime
from tqdm import tqdm

# MongoDB Connection Parameters
DB_NAME = "reddit"              # Replace with your database name
COLLECTION_NAME = "filtered_comments_standard"    # Replace with your collection name

# Connect to MongoDB
client = pymongo.MongoClient()
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def add_created_day_field():
    try:
        # Process each document in the collection
        for doc in tqdm(collection.find()):
            # Extract 'created_utc' field and convert it to a date string
            created_utc = doc.get('created_utc')
            if created_utc:
                created_day = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d')
                
                # Update the document with the new 'created_day' field
                collection.update_one({'_id': doc['_id']}, {'$set': {'created_day': created_day}})
        print("All documents have been updated with 'created_day' field.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function
add_created_day_field()
