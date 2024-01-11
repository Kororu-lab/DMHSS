import pymongo
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
import os

# Initialization Parameters
db_name = 'reddit'
collection_submissions = 'relevant_submissions'
collection_comments = 'relevant_comments'
output_dir = './analyze'
output_csv = os.path.join(output_dir, 'user_centric_analysis.csv')
output_viz = os.path.join(output_dir, 'user_sentiment_distribution.png')

def analyze_user_centric(db_name, collection_submissions, collection_comments, output_csv, output_viz):
    # MongoDB Connection
    client = pymongo.MongoClient()
    db = client[db_name]

    # Initialize Sentiment Analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Placeholder for aggregated results
    user_data = {}

    # Function to process a batch of documents
    def process_documents(documents):
        for doc in documents:
            # Ensure doc is a dictionary
            if not isinstance(doc, dict):
                print(f"Unexpected document format: {type(doc)}")
                continue

            author = doc.get('author')
            body = doc.get('body', '')
            if author and isinstance(body, str):
                if author not in user_data:
                    user_data[author] = {'texts': '', 'sentiment': 0.0, 'count': 0}
                user_data[author]['texts'] += ' ' + body
                sentiment = analyzer.polarity_scores(body)['compound']
                user_data[author]['sentiment'] += sentiment
                user_data[author]['count'] += 1

    # Fetch and process submissions
    submissions_cursor = db[collection_submissions].find({}, no_cursor_timeout=True)
    process_documents(submissions_cursor)

    # Fetch and process comments
    comments_cursor = db[collection_comments].find({}, no_cursor_timeout=True)
    process_documents(comments_cursor)

    # Convert user data to DataFrame
    df_user_data = pd.DataFrame.from_dict(user_data, orient='index')
    df_user_data['avg_sentiment'] = df_user_data['sentiment'] / df_user_data['count']

    # Save to CSV
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df_user_data.to_csv(output_csv)

    # Visualization
    plt.figure(figsize=(10, 6))
    df_user_data['avg_sentiment'].hist(bins=50)
    plt.title('User Average Sentiment Distribution')
    plt.xlabel('Average Sentiment Score')
    plt.ylabel('Number of Users')
    plt.savefig(output_viz)

    # Close the cursor
    submissions_cursor.close()
    comments_cursor.close()
    client.close()

# Running the analysis
analyze_user_centric(db_name, collection_submissions, collection_comments, output_csv, output_viz)
