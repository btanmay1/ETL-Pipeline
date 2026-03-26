import pandas as pd
import os

def extract():
    print("Extracting data...")
    df = pd.read_csv("data/raw_data.csv")
    return df

def transform(df):
    print("Transforming data...")
    df = df.dropna()
    df = df.drop_duplicates()

    # Example transformation
    df['value_normalized'] = (df['value'] - df['value'].mean()) / df['value'].std()

    return df

def load(df):
    print("Loading data...")
    os.makedirs("outputs", exist_ok=True)
    df.to_csv("outputs/processed_data.csv", index=False)
    print("Saved to outputs/processed_data.csv")

if __name__ == "__main__":
    df = extract()
    df = transform(df)
    load(df)
