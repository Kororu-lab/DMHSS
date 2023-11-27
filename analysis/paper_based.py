import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import cmudict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import multiprocessing as mp
from pymongo import MongoClient
from tqdm import tqdm
import matplotlib.pyplot as plt
from collections import Counter

# Hyperparameters
chunksize = 1000  # Adjust this based on your memory constraints

# MongoDB connection and configuration
DB_NAME = "reddit"
client = MongoClient()
db = client[DB_NAME]

# MongoDB and CSV settings
submission_collection_name = "filtered_submissions_sentiment"
comment_collection_name = "filtered_comments_sentiment"
csv_filename = "usr_submission.csv"

# Define subreddits
mh_subreddits = [
    "depression", "mentalhealth", "traumatoolbox", "bipolarreddit", 
    "BPD", "ptsd", "psychoticreddit", "EatingDisorders", "StopSelfHarm", 
    "survivorsofabuse", "rapecounseling", "hardshipmates", 
    "panicparty", "socialanxiety"
]
sw_subreddit = "SuicideWatch"

nltk.download('cmudict')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
d = cmudict.dict()

# Functions for analysis
def pos_tagging(text):    
    tokens = nltk.word_tokenize(text)
    tags = nltk.pos_tag(tokens)
    return Counter(tag for _, tag in tags)

def count_syllables(word):
    if word.lower() in d:
        return max([len(list(y for y in x if y[-1].isdigit())) for x in d[word.lower()]])
    else:
        # Fallback mechanism for words not found in the cmudict
        return sum(1 for char in word if char in 'aeiou')

def calculate_readability(text):
    syllable_count = sum(count_syllables(word) for word in word_tokenize(text))
    word_count = len(word_tokenize(text))
    sentence_count = len(nltk.sent_tokenize(text))
    flesch_kincaid_grade = 0.39 * (word_count/sentence_count) + 11.8 * (syllable_count/word_count) - 15.59
    return flesch_kincaid_grade

def compute_accommodation(prev_post, curr_post):
    vectorizer = TfidfVectorizer()
    tfidf = vectorizer.fit_transform([prev_post, curr_post])
    return cosine_similarity(tfidf)[0, 1]

def analyze_submissions(submissions, category):
    results = []
    for submission in submissions:
        if 'body' in submission:
            text = submission['body']  # For comments
        else:
            # For submissions, concatenate title and selftext (if they exist)
            title = submission.get('title', '')
            selftext = submission.get('selftext', '')
            text = title + " " + selftext

        pos_tags = pos_tagging(text)
        readability_score = calculate_readability(text)
        accommodation_score = 0  # Update this as per your logic

        # Collect the required tags
        noun_count = pos_tags.get('NN', 0) + pos_tags.get('NNS', 0)
        verb_adv_count = pos_tags.get('VB', 0) + pos_tags.get('RB', 0)
        first_person_pronouns = pos_tags.get('PRP', 0)  # Assuming 'PRP' includes first person pronouns
        other_pronouns = pos_tags.get('PRP$', 0)  # Assuming 'PRP$' includes other pronouns

        results.append({
            'noun_count': noun_count, 
            'verb_adv_count': verb_adv_count,
            'first_person_pronouns': first_person_pronouns,
            'other_pronouns': other_pronouns,
            'readability': readability_score, 
            'accommodation': accommodation_score
        })
    return results

def plot_results(results, category):
    # Data for plotting
    noun_count = [result['noun_count'] for result in results]
    verb_adv_count = [result['verb_adv_count'] for result in results]
    first_person_pronouns = [result['first_person_pronouns'] for result in results]
    other_pronouns = [result['other_pronouns'] for result in results]

    # Plotting
    plt.figure()
    plt.bar(range(len(noun_count)), noun_count, color='blue', label='Nouns')
    plt.bar(range(len(verb_adv_count)), verb_adv_count, color='green', label='Verbs + Adverbs')
    plt.legend()
    plt.title(f"Word Type Frequency in {category}")
    plt.savefig(f"{category}_word_type_freq.png")

    plt.figure()
    plt.bar(range(len(first_person_pronouns)), first_person_pronouns, color='red', label='1st Person Pronouns')
    plt.bar(range(len(other_pronouns)), other_pronouns, color='yellow', label='Other Pronouns')
    plt.legend()
    plt.title(f"Pronoun Frequency in {category}")
    plt.savefig(f"{category}_pronoun_freq.png")

# Load user list from CSV in chunks
user_list = []
for chunk in pd.read_csv(csv_filename, chunksize=chunksize):
    user_list.extend(chunk.iloc[:, 0].tolist())

# Modified process_category function
def process_category(category):
    if category == "MH":
        query = {"author": {"$in": user_list}, "subreddit": {"$in": mh_subreddits}}
    elif category == "all":
        query = {"author": {"$in": user_list}}
    else:
        query = {"author": {"$in": user_list}, "subreddit": category}

    # Initialize empty list to store aggregated results
    aggregated_results = []

    for collection_name in [submission_collection_name, comment_collection_name]:
        collection = db[collection_name]
        total_docs = collection.count_documents(query)
        pbar = tqdm(total=total_docs, desc=f"Processing {category} - {collection_name}")
        for chunk_start in range(0, total_docs, chunksize):
            documents = pd.DataFrame(list(collection.find(query).skip(chunk_start).limit(chunksize)))
            if documents.empty:
                break
            results = analyze_submissions(documents.to_dict('records'), category)
            aggregated_results.extend(results)
            pbar.update(len(documents))
        pbar.close()

    # After processing all chunks, plot results and save to PNG
    plot_results(aggregated_results, category)

    # Save results to CSV
    results_df = pd.DataFrame(aggregated_results)
    results_df.to_csv(f"{category}_analysis_results.csv", index=False)

# Parallel processing setup
categories = ["all", "MH", sw_subreddit, "others"]

pool = mp.Pool(mp.cpu_count())
for category in categories:
    pool.apply_async(process_category, args=(category,))
pool.close()
pool.join()

if __name__ == "__main__":
    # Load user list from CSV in chunks
    user_list = []
    for chunk in pd.read_csv(csv_filename, chunksize=chunksize):
        user_list.extend(chunk.iloc[:, 0].tolist())

    # Parallel processing setup
    categories = ["all", "MH", sw_subreddit, "others"]

    pool = mp.Pool(mp.cpu_count())
    for category in categories:
        pool.apply_async(process_category, args=(category,))
    pool.close()
    pool.join()
