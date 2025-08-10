import pandas as pd
import gzip

# Load a sample of the title.principals data
input_file = 'title.principals.tsv.gz'
with gzip.open(input_file, 'rt', encoding='utf-8') as f:
    df = pd.read_csv(f, delimiter='\t', low_memory=False, nrows=10000)

# Replace missing values
df.replace('\\N', pd.NA, inplace=True)

# Rename columns for clarity
df.rename(columns={
    'tconst': 'title_id',
    'nconst': 'name_id',
    'category': 'role_category',
    'job': 'job_description',
    'characters': 'characters'
}, inplace=True)

# Preview the result
print(df.head())

# Save cleaned data to temp file
output_path = '/tmp/title_principals_cleaned.csv'
df.to_csv(output_path, index=False)
print(f"Cleaned data saved to {output_path}")

