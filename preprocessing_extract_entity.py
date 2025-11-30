import pandas as pd
from underthesea import sent_tokenize, ner
from tqdm import tqdm

# Đọc file CSV
df = pd.read_csv('summary_paper.csv')

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
        for word, tag in ner(s):
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