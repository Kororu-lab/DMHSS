import multiprocessing as mp
mp.set_start_method('spawn', force=True)

from pymongo import MongoClient
from tqdm import tqdm
from multiprocessing import Pool
import general_sentiment  # Ensure this module is in your PYTHONPATH or in the same directory

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
COLLECTION_NAME = "relevant_submissions"
COLLECTION_TYPE = "S"  # 'C' for comments, 'S' for submissions
NUM_WORKERS = 4 # Number of parallel processes for data fetching

def get_text(doc):
    if COLLECTION_TYPE == "C":
        return doc.get("body", "").strip()
    elif COLLECTION_TYPE == "S":
        title = doc.get("title", "")
        selftext = doc.get("selftext", "")
        return (title + " " + selftext).strip()

def process_batch(batch):
    emotion_scores_batch = []

    for doc in batch:
        text = get_text(doc)
        if text:
            emotion_scores = general_sentiment.get_emotion_probabilities(text)
            emotion_scores_batch.append((doc["_id"], emotion_scores))
    
    return emotion_scores_batch

def process_chunk(worker_id, total_workers):
    local_client = MongoClient()
    local_db = local_client[DB_NAME]
    local_collection = local_db[COLLECTION_NAME]

    query = {"emotion_scores": {"$exists": False}}
    cursor = local_collection.find(query)
    processed_count = 0

    for i, doc in enumerate(tqdm(cursor, desc=f"Worker {worker_id}", position=worker_id)):
        if i % total_workers != worker_id:
            continue

        batch = process_batch([doc])
        for doc_id, emotion_scores in batch:
            update = {"$set": {"emotion_scores": emotion_scores}}
            local_collection.update_one({"_id": doc_id}, update)
            processed_count += 1

    local_client.close()
    print(f"Worker {worker_id} processed {processed_count} documents")

def main():
    with Pool(NUM_WORKERS) as pool:
        pool.starmap(process_chunk, [(worker_id, NUM_WORKERS) for worker_id in range(NUM_WORKERS)])

if __name__ == "__main__":
    main()
    print("Finished processing sentiment_general_update.")
