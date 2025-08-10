import pandas as pd
import gzip

# Load a small sample of the title.ratings data
input_file = 'title.ratings.tsv.gz'
with gzip.open(input_file, 'rt', encoding='utf-8') as f:
    df = pd.read_csv(f, delimiter='\t', low_memory=False, nrows=10000)

# Replace missing values (just in case)
df.replace('\\N', pd.NA, inplace=True)

# Rename columns
df.rename(columns={
    'tconst': 'title_id',
    'averageRating': 'average_rating',
    'numVotes': 'num_votes'
}, inplace=True)

# Convert numeric fields to proper types
df['average_rating'] = pd.to_numeric(df['average_rating'], errors='coerce')
df['num_votes'] = pd.to_numeric(df['num_votes'], errors='coerce').astype('Int64')

# Preview
print(df.head())

# Save to file
output_path = '/tmp/title_ratings_cleaned.csv'
df.to_csv(output_path, index=False)
print(f"Cleaned data saved to {output_path}")
