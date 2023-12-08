import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Directory where plots will be saved
plot_dir = "./step4/plots"

# Ensure the directory exists
os.makedirs(plot_dir, exist_ok=True)

# Load the data
df = pd.read_csv('./step4/user_interaction_scores.csv')

# Prepare data for matrix plot
def prepare_matrix_data(df):
    melted_df = df.melt(id_vars=['author'], var_name='category', value_name='score')
    split_data = melted_df['category'].str.split('-', expand=True)
    melted_df['subreddit_grp'] = split_data[0]
    melted_df['type'] = split_data[1]
    melted_df.dropna(subset=['score'], inplace=True)
    avg_scores = melted_df.groupby(['subreddit_grp', 'type']).agg({'score': 'mean'}).reset_index()
    return avg_scores

# Prepare data for time-wise plot
def prepare_time_data(df):
    # Melt the dataframe to long format
    df_melted = df.melt(id_vars=['author'], var_name='category', value_name='score')

    # Convert 'score' to numeric, remove non-numeric entries
    df_melted['score'] = pd.to_numeric(df_melted['score'], errors='coerce')
    df_melted.dropna(subset=['score'], inplace=True)

    # Print the first few rows of df_melted to check 'score' column
    print("DataFrame after melting and score conversion:")
    print(df_melted.head())

    # Split the 'category' column into subreddit group, type, and year_month
    split_data = df_melted['category'].str.split('-', expand=True)
    df_melted['subreddit_grp'] = split_data[0]
    df_melted['type'] = split_data[1]
    df_melted['year_month'] = split_data[2] + '-' + split_data[3]

    # Convert 'year_month' to datetime, remove invalid dates
    df_melted['year_month'] = pd.to_datetime(df_melted['year_month'], format='%Y-%m', errors='coerce')
    df_melted.dropna(subset=['year_month'], inplace=True)

    # Print the DataFrame before grouping
    # print("DataFrame before grouping:")
    # print(df_melted.head())

    # Group by subreddit group and year_month, calculate the average score
    grouped = df_melted.groupby(['subreddit_grp', 'year_month'])['score'].mean().reset_index()

    # Print the grouped data
    # print("Grouped data:")
    # print(grouped.head())

    return grouped

# Visualization functions
def create_matrix_plot(df):
    pivot_table = df.pivot_table(index='subreddit_grp', columns='type', values='score', fill_value=0)
    pivot_table = pivot_table.reindex(["SW", "MH", "Otr"], axis=0)  # Fix row order
    pivot_table = pivot_table.reindex(["sub", "com"], axis=1)  # Fix column order
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap='viridis')
    plt.title("Average Scores by Subreddit Group and Type")
    plt.ylabel("Subreddit Group")
    plt.xlabel("Type")
    plt.savefig(os.path.join(plot_dir, "matrix_plot.png"))

def create_time_wise_plot(df):
    plt.figure(figsize=(15, 7))
    sns.lineplot(data=df, x='year_month', y='score', hue='subreddit_grp')
    plt.title("Average Scores Over Time by Subreddit Group")
    plt.ylabel("Average Score")
    plt.xlabel("Year-Month")
    # Save the plot
    plt.savefig(os.path.join(plot_dir, "time_wise_plot.png"))

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
