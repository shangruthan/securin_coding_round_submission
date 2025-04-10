import pandas as pd
import json
import psycopg2
import os
from dotenv import load_dotenv

dbname=os.getenv('DB_NAME') or "securin"
user=os.getenv('DB_USER') or "postgres"
password=os.getenv('DB_PASSWORD') or "Shangruthan@05"
host=os.getenv('DB_HOST') or "localhost"
port=os.getenv('DB_PORT') or "5432"

with open("US_recipes_null.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

data = list(raw_data.values())
df = pd.DataFrame(data)

df = df.where(pd.notnull(df), None)

conn = psycopg2.connect(
         dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
    )
cursor = conn.cursor()

# cursor.execute("""
# CREATE TABLE IF NOT EXISTS recipes (
#     id SERIAL PRIMARY KEY,
#     cuisine TEXT,
#     title TEXT,
#     rating FLOAT,
#     prep_time INT,
#     cook_time INT,
#     total_time INT,
#     description TEXT,
#     nutrients JSONB,
#     serves TEXT
# )
# """)


insert_query = """
INSERT INTO recipes (
    cuisine, title, rating, prep_time, cook_time, total_time, description, nutrients, serves
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Function to convert NaN or missing to None
def safe(val, cast_fn=str, fallback=None):
    try:
        if pd.isna(val):
            return fallback
        return cast_fn(val)
    except:
        return fallback


for _, row in df.iterrows():
    try:
        values = (
            safe(row.get("cuisine")),
            safe(row.get("title")),
            safe(row.get("rating"), float),
            safe(row.get("prep_time"), int),
            safe(row.get("cook_time"), int),
            safe(row.get("total_time"), int),
            safe(row.get("description")),
            json.dumps(row["nutrients"]) if pd.notna(row.get("nutrients")) else json.dumps({}),
            safe(row.get("serves")),
        )
        cursor.execute(insert_query, values)
    except Exception as e:
        print(f"Skipping row due to error: {e}")
        continue


conn.commit()
cursor.close()
conn.close()

print(" All data inserted successfully!")