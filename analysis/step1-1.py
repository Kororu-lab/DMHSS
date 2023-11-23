import torch
import torch.nn as nn
from multiprocessing import Pool
import gc
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from collections import defaultdict, Counter
from tqdm import tqdm
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import csv
from sklearn.cluster import KMeans
import numpy as np
import seaborn as sns
from pymongo import MongoClient
import pymongo

# MongoDB connection and configuration
DB_NAME = "reddit"
COLLECTION_SUBMISSIONS = "filtered_submissions_sentiment"
COLLECTION_COMMENTS = "filtered_comments_sentiment"

# Constants
BATCH_SIZE = 64  # Define your batch size here
SEQUENCE_LENGTH = 10  # Define the length of sequences for LSTM

client = MongoClient()
db = client[DB_NAME]

# Define subreddit groupings
SUBREDDIT_GROUPS = {
    "SW": ["SuicideWatch"],
    "MH": ["depression", "Advice", "relationships", "Anxiety", "selfharm"],
    "MHRelated": ["Relationship_advice", "teenagers", "unpopularopinion", "NoStupidQuestions", "tifu"],
    "General": ["AskReddit", "gaming", "showerthoughts", "todayilearned", "mildlyinteresting", "WTF", "Amitheasshole", "IAmA", "explainlikeimfive", "politics", "trees", "interestingasfuck", "mildlyinfuriating", "AdviceAnimals", "tipofmytongue", "AskMen", "pcmasterrace", "trashy", "LifeProTips", "PublicFreakOut", "sex", "legalAdvice"],
    "Meme_Humor": ["Funny", "aww", "memes", "gifs", "dankmemes", "wholesomememes", "RoastMe", "jokes", "Me_irl", "facepalm"],
    "News_Media": ["pics", "videos", "worldnews", "movies", "Music", "news", "Minecraft"]
}

def process_data_batch(batch, subreddit_to_index, sequence_length):
    """
    Simplified processing of a batch of subreddit movement data for troubleshooting.
    """
    print(f"Processing batch of size: {len(batch)}")  # Add print statement for debugging
    sequences = []
    for author, subreddit in batch:
        subreddit_index = subreddit_to_index.get(subreddit, 0)
        sequences.append([subreddit_index] * sequence_length)  # Simplified sequence
    return sequences


def create_subreddit_index(user_movements):
    subreddit_to_index = {}
    current_index = 0
    for subreddit_list in user_movements.values():
        for subreddit in subreddit_list:
            if subreddit not in subreddit_to_index:
                subreddit_to_index[subreddit] = current_index
                current_index += 1
    return subreddit_to_index

def map_subreddit_to_group(subreddit):
    for group, subreddits in SUBREDDIT_GROUPS.items():
        if subreddit in subreddits:
            return group
    return "Other"

def fetch_and_process_data(collection_name):
    collection = db[collection_name]
    # Fetch only 'author' and 'subreddit' fields
    cursor = collection.find({}, {'author': 1, 'subreddit': 1, 'created_utc': 1})
    for doc in tqdm(cursor, desc=f"Processing {collection_name}"):
        yield doc['author'], doc['subreddit'], doc['created_utc']

class LSTMModel(nn.Module):
    def __init__(self, vocab_size, embed_dim, hidden_size, num_layers):
        super(LSTMModel, self).__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim)
        self.lstm = nn.LSTM(embed_dim, hidden_size, num_layers, batch_first=True)

    def forward(self, x):
        x = self.embedding(x)  # x should be [batch_size, sequence_length]
        # No need to unsqueeze since embedding layer will output [batch_size, sequence_length, embed_dim]
        out, (h_n, c_n) = self.lstm(x)
        return h_n[-1]  # Return the last hidden state as the feature vector


def preprocess_data_for_lstm(user_movements, subreddit_to_index, sequence_length=10):
    sequences = []
    for subreddit_list in user_movements.values():
        for i in range(len(subreddit_list) - sequence_length + 1):
            sequence = [subreddit_to_index.get(subreddit, 0) for subreddit in subreddit_list[i:i + sequence_length]]
            sequences.append(sequence)
    return torch.tensor(sequences, dtype=torch.long)

def train_lstm_model(model, data_loader, device, epochs=10):
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    for epoch in range(epochs):
        model.train()
        for batch in tqdm(data_loader, desc=f"Epoch {epoch+1}/{epochs}"):
            sequences = batch[0].to(device)  # Move sequences to the device

            # Debugging: Print the shape of the input tensor
            #print(f"Input tensor shape: {sequences.shape}")

            optimizer.zero_grad()

            # Forward pass through the model
            features = model(sequences)

            # Since we're not using a traditional target, we skip the loss calculation
            # If you decide to implement a self-supervised task, include the loss calculation here

            # Backpropagation
            # loss.backward()  # Uncomment if loss is calculated
            optimizer.step()


def analyze_lstm_results(model, data_loader, device):
    model.eval()
    all_features = []
    with torch.no_grad():
        for sequences in tqdm(data_loader, desc="Analyzing LSTM results"):
            sequences = sequences.to(device)
            outputs = model(sequences)
            all_features.extend(outputs[:, -1, :].cpu().numpy())

    # Cluster Analysis
    kmeans = KMeans(n_clusters=5, random_state=0).fit(all_features)
    return kmeans.labels_, all_features

def visualize_complex_movements(sequences, output_file='complex_movement_trends.png'):
    G = nx.DiGraph()
    for seq, count in sequences.items():
        G.add_edge(seq[0], seq[1], weight=count)

    plt.figure(figsize=(15, 15))
    pos = nx.spring_layout(G, k=0.15, iterations=20)
    nx.draw_networkx_edges(G, pos, edge_color='gray')
    nx.draw_networkx_labels(G, pos, font_size=8, labels={node: node for node in G.nodes()})
    nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=1000)
    plt.title("Network of Subreddit Transitions")
    plt.savefig(output_file)
    print(f"Network visualization saved to {output_file}")

def save_user_movements(user_movements, filename='user_movements.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['User', 'Movements'])
        for user, movements in user_movements.items():
            writer.writerow([user, ','.join(map(str, movements))])
    print(f"User movements saved to {filename}")

# Main Function
def main():
    try:
        client = pymongo.MongoClient()
        db = client[DB_NAME]

        # Fetch data and create subreddit visit sequences
        user_movements = defaultdict(list)
        print("Fetching and processing data...")
        # In your main function, fetching data:
        for collection_name in [COLLECTION_SUBMISSIONS, COLLECTION_COMMENTS]:
            for author, subreddit, _ in fetch_and_process_data(collection_name):
                user_movements[author].append(subreddit)

        # Create a mapping from subreddit names to indices
        subreddit_to_index = create_subreddit_index(user_movements)
        
        # Convert subreddit visits into sequences of indices
        sequences = []
        for subreddit_list in user_movements.values():
            if len(subreddit_list) >= SEQUENCE_LENGTH:
                for i in range(len(subreddit_list) - SEQUENCE_LENGTH + 1):
                    sequence = [subreddit_to_index[subreddit] for subreddit in subreddit_list[i:i + SEQUENCE_LENGTH]]
                    sequences.append(sequence)

        # Convert to tensor and create DataLoader
        sequence_tensor = torch.tensor(sequences, dtype=torch.long)
        print(f"Sequence tensor shape (before DataLoader): {sequence_tensor.shape}")
        preprocessed_data = torch.tensor(sequences, dtype=torch.long)
        data_loader = DataLoader(TensorDataset(sequence_tensor), batch_size=BATCH_SIZE, shuffle=True, num_workers=4)
        del user_movements, sequences
        gc.collect()  # Collect garbage

        # Debugging: Check the shape of a batch from the DataLoader
        for batch in DataLoader(TensorDataset(sequence_tensor), batch_size=BATCH_SIZE, shuffle=True):
            print(f"Batch shape (from DataLoader): {batch[0].shape}")
            break  # Just to test the first batch

        # LSTM Model setup and training
        vocab_size = len(subreddit_to_index)
        embed_dim = 50
        hidden_size = 128
        num_layers = 2
        epochs = 5
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = LSTMModel(vocab_size, embed_dim, hidden_size, num_layers).to(device)
        train_lstm_model(model, data_loader, device, epochs=epochs)

        # LSTM Analysis
        lstm_analysis_results = analyze_lstm_results(model, preprocessed_data, device)
        pd.DataFrame(lstm_analysis_results).to_csv('lstm_analysis_results.csv', index=False)

        # Advanced Visualization of LSTM Results (e.g., Cluster Visualization)
        # Assuming lstm_analysis_results contains cluster labels
        sns.clustermap(lstm_analysis_results, method='ward', cmap='viridis')
        plt.title("LSTM Clustering Results")
        plt.savefig('lstm_clustering.png')

        # Non-LSTM Analysis - Subreddit Transition Network Visualization
        sequences = Counter()
        for movement in user_movements.values():
            for i in range(len(movement) - 1):
                sequences[(movement[i], movement[i + 1])] += 1
        sequence_df = pd.DataFrame(sequences.items(), columns=['Transition', 'Count'])
        visualize_complex_movements(sequence_df, 'complex_movement_trends.png')
        sequence_df.to_csv('subreddit_transitions.csv', index=False)

        # Save User Movements
        with open('user_movements.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['User', 'Movements'])
            for user, movements in user_movements.items():
                writer.writerow([user, ','.join(movements)])

        print("Analysis complete.")

    except Exception as e:
        print(f"An error occurred in main: {e}")

if __name__ == "__main__":
    main()