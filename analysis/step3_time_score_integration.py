import pymongo
from tqdm import tqdm

# MongoDB Connection Parameters
DB_NAME = "reddit"
COMMENTS_COLLECTION = "filtered_comments_standard"
SUBMISSIONS_COLLECTION = "filtered_submissions_standard"
THRESHOLD_RATIO = 0.5  # Threshold for normalization (70%)
MIN_TIME_BASED_THRESHOLD = 0.01  # Minimum value for considering time_based score
OVERWRITE_EXISTING = True  # Set to False to skip documents with existing 'time_based_score'
SKIP_IF_EXISTS = False  # Set to True to skip updating documents that already have 'time_based_score'

client = pymongo.MongoClient()
db = client[DB_NAME]

def normalize_and_update_scores(collection_name):
    collection = db[collection_name]

    # Calculate threshold value for normalization
    threshold_value = collection.aggregate([
        {"$match": {"comm_score": {"$exists": True}}},
        {"$group": {"_id": None, "max_score": {"$max": "$comm_score"}}}
    ])
    threshold_value = next(threshold_value, {}).get('max_score', 0) * THRESHOLD_RATIO

    # Fetch documents and update scores
    cursor = collection.find({})
    for doc in tqdm(cursor, total=collection.count_documents({}), desc=f"Updating {collection_name}"):
        if SKIP_IF_EXISTS and 'time_based_score' in doc:
            continue

        comm_score = doc.get('comm_score', 0)
        time_based = doc.get('time_based', 0)

        # Normalize comm_score
        normalized_comm_score = min(comm_score / threshold_value, 1) if threshold_value != 0 else 0

        # Calculate final score and update document
        final_score = normalized_comm_score
        if time_based >= MIN_TIME_BASED_THRESHOLD:
            final_score += time_based

        update = {"$set": {"time_based_score": final_score}}
        collection.update_one({"_id": doc["_id"]}, update)

def main():
    normalize_and_update_scores(COMMENTS_COLLECTION)
    normalize_and_update_scores(SUBMISSIONS_COLLECTION)

if __name__ == "__main__":
    main()
