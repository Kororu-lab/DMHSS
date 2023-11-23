import pymongo
import pandas as pd
from multiprocessing import Pool, cpu_count
import csv
from tqdm import tqdm
import matplotlib.pyplot as plt

# Hyperparameters and Configurations
DB_NAME = "reddit"
COLLECTION_SUBMISSIONS = "filtered_submissions_sentiment"
COLLECTION_COMMENTS = "filtered_comments_sentiment"
OUTPUT_FILE = "user_sentiment_temperature.csv"
ROLLING_WINDOW_SIZE = 5
SIGNIFICANT_CHANGE_THRESHOLD = 0.5
GROUPINGS = {
    "SW": ["SuicideWatch"],
    "MH": ["depression", "Advice", "relationships", "Anxiety", "selfharm"],
    "MHRelated": ["Relationship_advice", "teenagers", "unpopularopinion", "NoStupidQuestions", "tifu"],
    "General": ["AskReddit", "gaming", "showerthoughts", "todayilearned", "mildlyinteresting", "WTF", "Amitheasshole", "IAmA", "explainlikeimfive", "politics", "trees", "interestingasfuck", "mildlyinfuriating", "AdviceAnimals", "tipofmytongue", "AskMen", "pcmasterrace", "trashy", "LifeProTips", "PublicFreakOut", "sex", "legalAdvice"],
    "Meme_Humor": ["Funny", "aww", "memes", "gifs", "dankmemes", "wholesomememes", "RoastMe", "jokes", "Me_irl", "facepalm"],
    "News_Media": ["pics", "videos", "worldnews", "movies", "Music", "news", "Minecraft"]
}

client = pymongo.MongoClient()
db = client[DB_NAME]

# Function to load and preprocess data
def load_and_preprocess_data():
    submissions = pd.DataFrame(list(db[COLLECTION_SUBMISSIONS].find({})))
    comments = pd.DataFrame(list(db[COLLECTION_COMMENTS].find({})))

    # Adjust to handle different sentiment score fields
    submissions = submissions[['author', 'sentiment_score_complex', 'created_utc', 'subreddit']]
    comments = comments[['author', 'sentiment_score', 'created_utc', 'subreddit']]

    combined_data = pd.concat([submissions.rename(columns={'sentiment_score_complex': 'sentiment_score'}),
                               comments])
    return combined_data

# Function to process user data
def process_user_data(user_data):
    # Group-wise analysis
    group_scores = {}
    for group_name, subreddits in GROUPINGS.items():
        group_data = user_data[user_data['subreddit'].isin(subreddits)]
        group_scores[group_name] = group_data['sentiment_score'].mean() if not group_data.empty else None

    # Compute average sentiment score
    average_score = user_data['sentiment_score'].mean()

    # Detect significant changes in sentiment
    rolling_mean = user_data['sentiment_score'].rolling(window=ROLLING_WINDOW_SIZE).mean()
    significant_change = (rolling_mean.diff().abs() > SIGNIFICANT_CHANGE_THRESHOLD).any()

    return {
        'user': user_data.name,
        'average_score': average_score,
        'significant_change': significant_change,
        'group_scores': group_scores
    }

# Function for visualization and summary statistics
def visualize_and_summarize(processed_data):
    df = pd.DataFrame(processed_data)

    # Histogram of average scores
    plt.hist(df['average_score'].dropna(), bins=20, color='blue', alpha=0.7)
    plt.title("Histogram of Average Sentiment Scores")
    plt.xlabel("Average Sentiment Score")
    plt.ylabel("Frequency")
    plt.show()

    # Summary statistics
    print(df.describe())

# Load and preprocess data
data = load_and_preprocess_data()
grouped_data = data.groupby('author')

# Process user data in parallel with progress visualization
with Pool(cpu_count()) as pool:
    # Convert grouped data iterator to a list for tqdm
    grouped_list = list(grouped_data)
    # Use tqdm with imap for progress tracking
    processed_data = list(tqdm(pool.imap(process_user_data, (group for _, group in grouped_list)), total=len(grouped_list)))

# Visualization and summary
visualize_and_summarize(processed_data)

# Export results to CSV
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['User', 'Average_Score', 'Significant_Change', 'SW_Score', 'MH_Score'])
    for record in processed_data:
        writer.writerow([
            record['user'], 
            record['average_score'], 
            record['significant_change'],
            record['group_scores'].get('SW'),
            record['group_scores'].get('MH')
        ])

print(f"Analysis completed and results saved to {OUTPUT_FILE}.")
