import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Load the data
file_path = Path('./step4/user_interaction_scores.csv')  # Adjust the file path as needed
df = pd.read_csv(file_path)

# Prepare data for matrix plot
def prepare_matrix_data(df):
    # Drop 'author' column and calculate mean for each subreddit type
    df.drop('author', axis=1, inplace=True)
    avg_scores = df.mean().reset_index()
    avg_scores[['subreddit_grp', 'type']] = avg_scores['index'].str.rsplit('-', n=1, expand=True)
    avg_scores.drop('index', axis=1, inplace=True)
    return avg_scores.groupby(['subreddit_grp', 'type']).mean().reset_index()

# Prepare data for time-wise plot
def prepare_time_data(df):
    # Melt the dataframe to long format
    df_melted = df.melt(id_vars=['author'], var_name='category', value_name='score')
    df_melted[['subreddit_grp', 'type', 'month']] = df_melted['category'].str.split('-', expand=True)
    df_melted.drop(['author', 'category', 'type'], axis=1, inplace=True)
    df_melted = df_melted.dropna(subset=['score'])
    df_melted['month'] = pd.to_datetime(df_melted['month'], errors='coerce', format='%Y-%m').dt.to_period('M')
    return df_melted.groupby(['subreddit_grp', 'month']).mean().reset_index()

# Visualization functions
def create_matrix_plot(df):
    pivot_table = df.pivot_table(index='subreddit_grp', columns='type', values='score', fill_value=0)
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap='viridis')
    plt.title("Average Scores by Subreddit Group and Type")
    plt.ylabel("Subreddit Group")
    plt.xlabel("Type")
    plt.show()


def create_time_wise_plot(df):
    sns.lineplot(data=df, x='month', y='score', hue='subreddit_grp')
    plt.title("Average Score Over Time by Subreddit Group")
    plt.xticks(rotation=45)
    plt.show()

# Main function
def main():
    avg_matrix_data = prepare_matrix_data(df)
    create_matrix_plot(avg_matrix_data)

    avg_time_data = prepare_time_data(df)
    if not avg_time_data.empty:
        create_time_wise_plot(avg_time_data)
    else:
        print("No data available for time-wise plot.")

if __name__ == "__main__":
    main()
