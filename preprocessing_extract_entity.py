import pandas as pd
import uuid
from underthesea import sent_tokenize, ner
from tqdm import tqdm
import psycopg2
import os

# DB connection params 
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

def get_max_id():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(id) FROM paper")
                row = cursor.fetchone()
                print(f"Max ID: {row[0]}")
                return int(row[0]) if row and row[0] else None
    except Exception as e:
        print("Error:", e)
        return None

# Query ID for paper
def query_id(url: str):
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                sql = "SELECT id FROM paper WHERE url = %s"
                cursor.execute(sql, (url,))
                row = cursor.fetchone()
                return int(row[0]) if row else None
    except Exception as e:
        print("Error:", e)
        return None

# Đọc file CSV
df = pd.read_csv('summary_paper.csv')

# Lấy ID lớn nhất hiện tại từ DB
current_max_id = get_max_id()
fallback_start = max(current_max_id + 1 if current_max_id else 0, 10**12)

# Bộ nhớ ID đã dùng để tránh trùng
used_ids = set()

# Hàm cấp ID mới
def get_safe_int_id(url):
    global fallback_start

    db_id = query_id(url)
    if db_id is not None:
        used_ids.add(db_id)
        return db_id

    # fallback
    while fallback_start in used_ids:
        fallback_start += 1

    new_id = fallback_start
    used_ids.add(new_id)
    fallback_start += 1

    return new_id

# ============================================
# CHECK & ADD ID IF MISSING
# ============================================
if "id" not in df.columns:
    df["id"] = [get_safe_int_id(url) for url in df["URL"]]
    print("✔ Added 'id' column using DB ID + safe fallback")
else:
    print("✔ 'id' already exists")

# ============================================
# CHUẨN HÓA TÊN CỘT - Chuyển tất cả về lowercase
# ============================================
df.columns = df.columns.str.lower()
print("✓ Các cột sau khi chuẩn hóa:", df.columns.tolist())

# Làm sạch text
def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).replace("\n", " ").strip()
    return text

# Áp dụng cho title + content (giờ dùng lowercase)
df["content_clean"] = df["content"].apply(clean_text)
df["title_clean"] = df["title"].apply(clean_text)

# Tách câu từ content
df["sentences"] = df["content_clean"].apply(lambda x: sent_tokenize(x))


# --- Hàm trích xuất entity từ 1 bài báo ---
def extract_entities_from_sentences(sentences):
    entities = []
    for s in sentences:
        for item in ner(s):
            word, tag = item[0], item[1]
            if tag != "O":
                entities.append({
                    "entity": word,
                    "entity_type": tag,
                    "sentence": s
                })
    return entities


# --- Xử lý toàn bộ DataFrame ---
def process_all(df, output_csv):
    all_entities = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Extracting entities"):
        entities = extract_entities_from_sentences(row["sentences"])
        for e in entities:
            all_entities.append({
                "article_id": row["id"],
                "entity": e["entity"],
                "entity_type": e["entity_type"],
                "sentence": e["sentence"],
                "source": row["source"],
                "category": row["category"],
                "date": row["time"]
            })

    result_df = pd.DataFrame(all_entities)
    result_df.to_csv(output_csv, index=False)
    print(f"✔ Done! Saved {len(result_df)} entities to: {output_csv}")


# Chạy xử lý toàn bộ file
process_all(df, "extracted_entities.csv")