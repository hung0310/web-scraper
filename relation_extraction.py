import pandas as pd
import re
from itertools import combinations
from neo4j import GraphDatabase
from tqdm import tqdm
import json

# Load relation patterns
with open("relation_vocab.json", "r", encoding="utf-8") as f:
    RELATION_PATTERNS = json.load(f)

# Hàm xác định loại quan hệ dựa vào từ khóa
def detect_relation(sentence):
    for rel, patterns in RELATION_PATTERNS.items():
        for p in patterns:
            if re.search(p, sentence.lower()):
                return rel
    return None

# Kiểm tra entity hợp lệ
def is_valid_entity(text):
    """
    Entity hợp lệ phải chứa ít nhất 1 ký tự chữ cái hoặc chữ số.
    Loại bỏ các entity chỉ chứa ký tự đặc biệt.
    """
    if not text or not text.strip():
        return False
    return bool(re.search(r'\w', text))

# Chuẩn hóa relation type cho Neo4j
def normalize_relation_type(rel_type):
    """
    Chuẩn hóa relation type để hợp lệ với Neo4j:
    - Không được bắt đầu bằng số
    - Chỉ chứa chữ cái, số, underscore
    - Viết hoa toàn bộ
    """
    rel_type = str(rel_type).strip()
    
    if rel_type and rel_type[0].isdigit():
        rel_type = "REL_" + rel_type
    
    rel_type = re.sub(r'[^a-zA-Z0-9_]', '_', rel_type)
    rel_type = rel_type.upper()
    
    return rel_type if rel_type else "UNKNOWN_RELATION"

# Gửi batch quan hệ vào Neo4j
def write_relation_batch(tx, rows, progress_bar=None):
    """Gửi batch quan hệ vào Neo4j với progress bar"""
    relation_groups = {}
    
    for row in rows:
        rel_type = normalize_relation_type(row['relation'])
        if rel_type not in relation_groups:
            relation_groups[rel_type] = []
        relation_groups[rel_type].append(row)
    
    for rel_type, rel_rows in relation_groups.items():
        query = f"""
        UNWIND $rows AS row
        MATCH (a:Entity {{name: row.e1}})
        MATCH (b:Entity {{name: row.e2}})
        MERGE (a)-[r:{rel_type}]->(b)
        ON CREATE SET r.example = row.sentence
        """
        tx.run(query, rows=rel_rows)
    
    if progress_bar:
        progress_bar.update(len(rows))

# ============================================
# MAIN PROCESSING
# ============================================

print("Bắt đầu trích xuất quan hệ từ extracted_entities.csv...")

# Đọc file CSV
df = pd.read_csv("extracted_entities.csv")

# Lọc theo date từ tháng 11 trở đi
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df[df["date"].dt.month >= 11]

print(f"Số dòng sau khi lọc: {len(df)}")

# Trích xuất quan hệ
rows = []
grouped = df.groupby("sentence")

print("Đang trích xuất quan hệ...")
for sent, group in tqdm(grouped, desc="Processing sentences"):
    entities = group["entity"].unique().tolist()
    
    if len(entities) < 2:
        continue
    
    rel = detect_relation(sent)
    if not rel:
        continue
    
    for e1, e2 in combinations(entities, 2):
        # Kiểm tra entity hợp lệ
        if not is_valid_entity(e1) or not is_valid_entity(e2):
            continue
        
        rows.append({
            "e1": e1.strip(),
            "e2": e2.strip(),
            "relation": rel.strip(),
            "sentence": sent.strip()
        })

# Tạo DataFrame và lưu file
rel_df = pd.DataFrame(rows)
rel_df.to_csv("relations.csv", index=False)
print(f"✓ Đã tạo file relations.csv với {len(rel_df)} quan hệ.")

# ============================================
# GỬI QUAN HỆ VÀO NEO4J
# ============================================

# Kết nối Neo4j
uri = "bolt://52.77.239.109:7687"
user = "neo4j"
password = "akeneo4jpw"
driver = GraphDatabase.driver(uri, auth=(user, password))

chunk_size = 50_000
batch_size = 10_000

print("\nBắt đầu gửi quan hệ vào Neo4j...")

with driver.session() as session:
    chunk_reader = pd.read_csv("relations.csv", chunksize=chunk_size)
    
    for chunk_idx, chunk in enumerate(chunk_reader, start=1):
        print(f"\nĐang xử lý chunk {chunk_idx} ({len(chunk)} dòng)...")
        
        batch_rows = []
        for _, row in chunk.iterrows():
            e1 = str(row.get("e1", "")).strip()
            e2 = str(row.get("e2", "")).strip()
            relation = str(row.get("relation", "")).strip()
            sentence = str(row.get("sentence", "")).strip()
            
            if not e1 or not e2 or not relation:
                continue
            
            batch_rows.append({
                "e1": e1,
                "e2": e2,
                "relation": relation,
                "sentence": sentence
            })
        
        if not batch_rows:
            print(f"Chunk {chunk_idx}: Không có dữ liệu hợp lệ")
            continue
        
        print(f"Gửi {len(batch_rows)} quan hệ vào Neo4j...")
        with tqdm(
            total=len(batch_rows),
            desc=f"Chunk {chunk_idx}",
            unit="relations",
            ncols=100
        ) as pbar:
            for i in range(0, len(batch_rows), batch_size):
                mini_batch = batch_rows[i:i + batch_size]
                session.execute_write(write_relation_batch, mini_batch, pbar)
        
        print(f"✓ Hoàn tất chunk {chunk_idx}")

driver.close()
print("\n✓ Hoàn tất tất cả!")