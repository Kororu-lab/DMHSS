from pymongo import MongoClient
from kaggle_model_1 import get_label_probabilities # kaggle_model_1
# from twitter_sentiment import get_label_probabilities # twitter model
# from kaggle_model_2 import get_label_probabilities # kaggle_model_2
from tqdm import tqdm
from multiprocessing import Pool, current_process

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
COLLECTION_NAME = "filtered_comments_score"
COLLECTION_TYPE = "C"  # 'C' for comments, 'S' for submissions
BATCH_SIZE = 40  # Adjust based on available VRAM and model size
NUM_WORKERS = 4  # Number of parallel processes for data fetching

client = MongoClient()
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def process_comment_batch(batch):
    sentiment_scores = []
    for doc in batch:
        text = str(doc.get("body", ""))
        sentiment_prob = get_label_probabilities(text)[1]
        sentiment_scores.append(sentiment_prob)
    return sentiment_scores

def process_submission_batch(batch):
    title_sentiment_scores = []
    selftext_sentiment_scores = []
    combined_sentiment_scores = []

    for doc in batch:
        title = doc.get("title", "")  # Default to empty string if missing
        selftext = doc.get("selftext", "")  # Default to empty string if missing

        # Ensure both title and selftext are strings
        title = str(title) if title else ""
        selftext = str(selftext) if selftext else ""

        title_sentiment_prob = get_label_probabilities(title)[1]
        selftext_sentiment_prob = get_label_probabilities(selftext)[1] if selftext.strip() else 0
        combined_sentiment_prob = get_label_probabilities(title + " " + selftext)[1]

        title_sentiment_scores.append(title_sentiment_prob)
        selftext_sentiment_scores.append(selftext_sentiment_prob)
        combined_sentiment_scores.append(combined_sentiment_prob)

    return title_sentiment_scores, selftext_sentiment_scores, combined_sentiment_scores

def process_chunk(chunk_start, chunk_end):
    local_client = MongoClient()
    local_db = local_client[DB_NAME]
    local_collection = local_db[COLLECTION_NAME]
    cursor = local_collection.find({}).skip(chunk_start).limit(chunk_end - chunk_start)

    # Using tqdm with postfix to identify which process is providing updates
    for doc in tqdm(cursor, desc=f"Process {current_process().name}", position=int(current_process().name.split('-')[-1])):
        if COLLECTION_TYPE == "C":
            score = process_comment_batch([doc])[0]
            local_collection.update_one({"_id": doc["_id"]}, {"$set": {"sentiment_score": float(score)}})
        else:
            title_score, selftext_score, combined_score = process_submission_batch([doc])
            local_collection.update_one({"_id": doc["_id"]}, {"$set": {
                "sentiment_score_title": float(title_score[0]),
                "sentiment_score_text": float(selftext_score[0]),
                "sentiment_score_complex": float(combined_score[0])
            }})

    local_client.close()

# Calculate the number of documents to process per worker
total_docs = collection.count_documents({})
docs_per_worker = total_docs // NUM_WORKERS

# Create a list of (start, end) indices for each chunk
chunks = [(i, min(i + docs_per_worker, total_docs)) for i in range(0, total_docs, docs_per_worker)]

# Process each chunk in parallel
with Pool(NUM_WORKERS) as pool:
    results = pool.starmap(process_chunk, chunks)

# Check for None results and handle them
cleaned_results = [res for res in results if res is not None]

# Flatten the cleaned results
flat_results = [item for sublist in cleaned_results for item in sublist]

# TODO: Store the results or perform further processing

# Close the main connection
client.close()

print("Finished processing.")
