import pandas as pd
import gzip

# Load a small sample of the name.basics data
input_file = 'name.basics.tsv.gz'
with gzip.open(input_file, 'rt', encoding='utf-8') as f:
    df = pd.read_csv(f, delimiter='\t', low_memory=False, nrows=10000)

# Replace missing values
df.replace('\\N', pd.NA, inplace=True)

# Rename columns for clarity
df.rename(columns={
    'nconst': 'name_id',
    'primaryName': 'name',
    'birthYear': 'birth_year',
    'deathYear': 'death_year',
    'primaryProfession': 'professions',
    'knownForTitles': 'known_for_titles'
}, inplace=True)

# Convert birth and death years to integers where possible
for col in ['birth_year', 'death_year']:
    df[col] = pd.to_numeric(df[col], errors='coerce').dropna().astype('Int64')

# Preview cleaned data
print(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns.")
print(df.head())

# Save cleaned data to a CSV file in the temp directory
output_path = '/tmp/name_basics_cleaned.csv'
df.to_csv(output_path, index=False)
print(f"Cleaned data saved to {output_path}")