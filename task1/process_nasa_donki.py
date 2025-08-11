import requests, pandas as pd, json, csv, re

API_KEY = "d5XJXY7LUBjy8TavGWK1eYa7cMLjqHxjjYqMF915"
START_DATE = "2023-01-01"
END_DATE = "2023-12-31"
URL = f"https://api.nasa.gov/DONKI/FLR?startDate={START_DATE}&endDate={END_DATE}&api_key={API_KEY}"

r = requests.get(URL)
r.raise_for_status()
data = r.json()

if not data:
    print("No solar flare data.")
    raise SystemExit(0)

df = pd.json_normalize(data)

# Ensure nested/list/dict cols are JSON strings (double quotes), not Python reprs
nested_cols = []
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
        nested_cols.append(col)

for col in nested_cols:
    df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

# Clean text fields: remove newlines that break CSV rows
text_like = df.select_dtypes(include=['object']).columns.tolist()
for col in text_like:
    df[col] = df[col].astype(str).apply(lambda s: re.sub(r'[\r\n]+', ' ', s))

# Write CSV with robust quoting
out_path = "/tmp/nasa_solar_flares_2023.csv"
df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL, escapechar='\\', doublequote=True)
print(f"Wrote cleaned CSV: {out_path}")
