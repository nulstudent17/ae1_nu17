import pandas as pd
import gzip

# Load a small sample
input_file = 'title.basics.tsv.gz'
with gzip.open(input_file, 'rt', encoding='utf-8') as f:
    df = pd.read_csv(f, delimiter='\t', low_memory=False, nrows=10000)

# Replace IMDB's placeholder for missing values
df.replace('\\N', pd.NA, inplace=True)

# Rename columns
df.rename(columns={
    'tconst': 'title_id',
    'titleType': 'type',
    'primaryTitle': 'title',
    'originalTitle': 'original_title',
    'startYear': 'start_year',
    'endYear': 'end_year',
    'runtimeMinutes': 'runtime_minutes'
}, inplace=True)

# Convert year and runtime fields to proper integers
for col in ['start_year', 'end_year', 'runtime_minutes']:
    df[col] = pd.to_numeric(df[col], errors='coerce').dropna().astype('Int64')

# Show final preview
print(df.head())

# Save cleaned sample to temporary file
output_path = '/tmp/title_basics_cleaned.csv'
df.to_csv(output_path, index=False)
print(f"Cleaned data saved to {output_path}")