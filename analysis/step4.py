import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
import torch
from torch_geometric.data import Data
import scipy.sparse as sp
from tqdm import tqdm
import pickle
import csv
import networkx as nx
import matplotlib.pyplot as plt
import torch.nn.functional as F
from torch_geometric.nn import GCNConv

# MongoDB connection
client = MongoClient()
db = client['reddit']
comments_collection = db['filtered_comments_sentiment']
submissions_collection = db['filtered_submissions_sentiment']

# Function to convert MongoDB data to DataFrame with selective field loading
def mongo_to_dataframe(collection, required_fields, batch_size=10000):
    all_documents = []
    cursor = collection.find({}, required_fields).batch_size(batch_size)
    for document in tqdm(cursor, desc=f"Loading {collection.name}"):
        all_documents.append(document)
    return pd.DataFrame(all_documents)

# Fields to be loaded from the collections
required_fields = {'author': 1, 'created_utc': 1, 'parent_id': 1, '_id': 0}

# Data extraction with batch processing and selective field loading
comments = mongo_to_dataframe(comments_collection, required_fields)
submissions = mongo_to_dataframe(submissions_collection, required_fields)

# Combine comments and submissions
df = pd.concat([comments, submissions])

# Graph Construction
def construct_graph(df):
    author_to_index = {author: idx for idx, author in enumerate(df['author'].unique())}
    edges = []
    for _, row in tqdm(df.iterrows(), desc="Processing interactions", total=df.shape[0]):
        if 'parent_id' in row:
            parent_id = row['parent_id']
            child_author = row['author']
            parent_row = df.loc[df['_id'] == parent_id]
            if not parent_row.empty:
                parent_author = parent_row['author'].values[0]
                edges.append((author_to_index[parent_author], author_to_index[child_author]))
    edge_index = torch.tensor(edges, dtype=torch.long).t().contiguous()
    return edge_index

edge_index = construct_graph(df)

# Node features can be incorporated as needed. For now, we will use a simple feature for each node.
num_nodes = len(df['author'].unique())
node_features = torch.eye(num_nodes)  # Simple identity features

data = Data(x=node_features, edge_index=edge_index)


# GNN Model
class GNN(torch.nn.Module):
    def __init__(self):
        super(GNN, self).__init__()
        self.conv1 = GCNConv(data.num_features, 16)
        self.conv2 = GCNConv(16, 2)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, training=self.training)
        x = self.conv2(x, edge_index)
        return F.log_softmax(x, dim=1)

# GNN Training
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = GNN().to(device)
data = data.to(device)

optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
model.train()

for epoch in range(200):
    optimizer.zero_grad()
    out = model(data)
    loss = F.nll_loss(out, data.y)  # This needs to be adjusted according to your specific task
    loss.backward()
    optimizer.step()

# Save the graph to a file
with open('reddit_network_graph.pkl', 'wb') as f:
    pickle.dump(data, f)

# Generate a CSV summary report
with open('network_summary.csv', 'w', newline='') as csvfile:
    fieldnames = ['Metric', 'Value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'Metric': 'Number of Nodes', 'Value': data.num_nodes})
    writer.writerow({'Metric': 'Number of Edges', 'Value': data.num_edges})

# Optional: Basic Visualization
try:
    subgraph_data = Data(edge_index=data.edge_index[:, :1000])
    subgraph = torch_geometric.utils.to_networkx(subgraph_data, to_undirected=True)
    plt.figure(figsize=(10, 10))
    nx.draw(subgraph, with_labels=False, node_size=50)
    plt.savefig('network_visualization.png')
    plt.close()
except Exception as e:
    print(f"Visualization error: {e}")

client.close()
