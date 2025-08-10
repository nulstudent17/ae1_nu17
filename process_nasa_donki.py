import requests
import pandas as pd

# NASA DONKI API configuration
API_KEY = "d5XJXY7LUBjy8TavGWK1eYa7cMLjqHxjjYqMF915"  # Your real key
START_DATE = "2023-01-01"
END_DATE = "2023-12-31"
URL = f"https://api.nasa.gov/DONKI/FLR?startDate={START_DATE}&endDate={END_DATE}&api_key={API_KEY}"

# Fetch data from NASA
response = requests.get(URL)
if response.status_code != 200:
    print(f"Error: {response.status_code} - {response.text}")
    exit()

data = response.json()

# Convert JSON to pandas DataFrame
if not data:
    print("No solar flare data returned.")
    exit()

df = pd.json_normalize(data)

# Preview data
print(f"Loaded {len(df)} solar flare records.")
print(df.head())

# Save cleaned data to CSV
output_path = "/tmp/nasa_solar_flares_2023.csv"
df.to_csv(output_path, index=False)
print(f"Cleaned data saved to {output_path}")

