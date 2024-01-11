from pymongo import MongoClient
from nltk import pos_tag, word_tokenize, download
from textstat import flesch_reading_ease
from collections import Counter, defaultdict
from tqdm import tqdm
import csv
from datetime import datetime

# Ensure required NLTK resources are downloaded
download('punkt')
download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()
db = client['reddit']

# Collection names
submissions_collection_name = "relevant_submissions"
comments_collection_name = "relevant_comments"

def get_date_from_utc(utc):
    """ Convert UTC timestamp to YYYY-MM-DD format """
    return datetime.fromtimestamp(int(utc)).strftime('%Y-%m-%d')

def process_text(text):
    """ Process text and return POS counts, text length, readability, and word frequencies """
    words = word_tokenize(text)
    pos_tags = pos_tag(words)
    pos_counts = Counter(tag for _, tag in pos_tags)
    word_freq = Counter(words)

    text_length = len(words)
    readability_score = flesch_reading_ease(text)

    return pos_counts, text_length, readability_score, word_freq

def aggregate_data(collection_name):
    collection = db[collection_name]
    cursor = collection.find({}, {'created_utc': 1, 'author': 1, 'body': 1, 'title': 1, 'selftext': 1, 'subreddit_grp': 1, 'user_info.user_grp': 1})
    
    aggregated_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        'pos_counts': Counter(),
        'word_freq': Counter(),
        'text_length': 0,
        'readability_score': 0
    })))

    for document in tqdm(cursor, desc=f'Processing {collection_name}'):
        date = get_date_from_utc(document['created_utc'])
        user_group = document['user_info']['user_grp']
        subreddit_group = document['subreddit_grp']
        
        # Check for the appropriate text field (body for comments, title+selftext for submissions)
        if 'body' in document:
            text = document['body']
        else:
            title = document.get('title', '')
            selftext = document.get('selftext', '')
            text = title + ' ' + selftext

        pos_counts, text_length, readability_score, word_freq = process_text(text)

        agg = aggregated_data[date][user_group][subreddit_group]
        agg['pos_counts'].update(pos_counts)
        agg['word_freq'].update(word_freq)
        agg['text_length'] += text_length
        agg['readability_score'] += readability_score

    return aggregated_data

# Aggregate data from submissions and comments
aggregated_submissions = aggregate_data(submissions_collection_name)
aggregated_comments = aggregate_data(comments_collection_name)

# Exporting to CSV
def export_to_csv(aggregated_data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'User Group', 'Subreddit Group', 'POS Tag', 'Count', 'Word', 'Frequency', 'Total Text Length', 'Total Readability Score'])

        for date, user_groups in aggregated_data.items():
            for user_group, subreddit_groups in user_groups.items():
                for subreddit_group, metrics in subreddit_groups.items():
                    for pos, count in metrics['pos_counts'].items():
                        word, freq = metrics['word_freq'].most_common(1)[0] if metrics['word_freq'] else ('', 0)
                        writer.writerow([date, user_group, subreddit_group, pos, count, word, freq, metrics['text_length'], metrics['readability_score']])

export_to_csv(aggregated_submissions, 'aggregated_submissions.csv')
export_to_csv(aggregated_comments, 'aggregated_comments.csv')
