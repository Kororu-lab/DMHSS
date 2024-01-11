from pymongo import MongoClient
from depression import get_label_probabilities  # Replace with your actual sentiment analysis model
from tqdm import tqdm
from multiprocessing import Pool

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
COLLECTION_NAME = "relevant_submissions"
COLLECTION_TYPE = "S"  # 'C' for comments, 'S' for submissions
NUM_WORKERS = 2  # Number of parallel processes for data fetching

def process_comment_batch(batch):
    sentiment_scores = []
    for doc in batch:
        text = str(doc.get("body", ""))
        sentiment_prob = get_label_probabilities(text)[0]
        sentiment_scores.append(sentiment_prob)
    return sentiment_scores

def process_submission_batch(batch):
    title_sentiment_scores = []
    selftext_sentiment_scores = []
    combined_sentiment_scores = []

    for doc in batch:
        title = str(doc.get("title", ""))
        selftext = str(doc.get("selftext", ""))

        title_sentiment_prob = get_label_probabilities(title)[0] if title else 0
        selftext_sentiment_prob = get_label_probabilities(selftext)[0] if selftext else 0
        combined_sentiment_prob = get_label_probabilities(title + " " + selftext)[0] if title or selftext else 0

        title_sentiment_scores.append(float(title_sentiment_prob))
        selftext_sentiment_scores.append(float(selftext_sentiment_prob))
        combined_sentiment_scores.append(float(combined_sentiment_prob))

    return title_sentiment_scores, selftext_sentiment_scores, combined_sentiment_scores

def process_chunk(worker_id, total_workers):
    local_client = MongoClient()
    local_db = local_client[DB_NAME]
    local_collection = local_db[COLLECTION_NAME]

    cursor = local_collection.find({})
    processed_count = 0

    for i, doc in enumerate(tqdm(cursor, desc=f"Worker {worker_id}", position=worker_id)):
        if i % total_workers != worker_id:
            continue  # Skip documents that are not assigned to this worker

        # Check if sentiment scores already exist
        if COLLECTION_TYPE == "C":
            if "sentiment_score" in doc:
                continue  # Skip this document as it's already processed
        else:
            if all(key in doc for key in ["sentiment_score_title", "sentiment_score_text", "sentiment_score_complex"]):
                continue  # Skip this document as it's already processed

        # Processing logic
        if COLLECTION_TYPE == "C":
            score = process_comment_batch([doc])[0]
            local_collection.update_one({"_id": doc["_id"]}, {"$set": {"sentiment_score": score}})
        else:
            title_score, selftext_score, combined_score = process_submission_batch([doc])
            local_collection.update_one({"_id": doc["_id"]}, {"$set": {
                "sentiment_score_title": title_score[0],
                "sentiment_score_text": selftext_score[0],
                "sentiment_score_complex": combined_score[0]
            }})

        processed_count += 1

    local_client.close()
    print(f"Worker {worker_id} processed {processed_count} documents")

def main():
    with Pool(NUM_WORKERS) as pool:
        pool.starmap(process_chunk, [(worker_id, NUM_WORKERS) for worker_id in range(NUM_WORKERS)])

if __name__ == "__main__":
    main()
    print("Finished processing.")
