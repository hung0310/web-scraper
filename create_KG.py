from pathlib import Path
from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import json

def normalize(value, default=""):
    if value is None or pd.isna(value):
        return default
    text = str(value).strip()
    return text if text else default

# Hàm quản lý checkpoint
def get_checkpoint(checkpoint_file):
    """Đọc checkpoint từ file"""
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_checkpoint(checkpoint_file, csv_name, chunk_idx):
    """Lưu checkpoint vào file"""
    checkpoint = get_checkpoint(checkpoint_file)
    checkpoint[csv_name] = chunk_idx
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)
        
def write_batch(tx, rows, progress_bar=None):
    """Gửi batch records với cập nhật progress bar"""
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (a:Article {id: row.article_id})
          ON CREATE SET a.title = row.title, a.date = row.date
          ON MATCH SET a.title = coalesce(row.title, a.title),
                       a.date = coalesce(row.date, a.date)
        MERGE (s:Source {name: row.source})
        MERGE (c:Category {name: row.category})
        MERGE (e:Entity {name: row.entity, type: row.entity_type})
        MERGE (a)-[:MENTIONS]->(e)
        MERGE (a)-[:PUBLISHED_BY]->(s)
        MERGE (a)-[:HAS_CATEGORY]->(c)
        """,
        rows=rows,
    )
    if progress_bar:
        progress_bar.update(len(rows))
        
# Kết nối Neo4j
uri = "bolt://52.77.239.109:7687"
user = "neo4j"
password = "akeneo4jpw"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Chỉ sửa: csv_paths thành list of Path
csv_paths = [Path("extracted_entities.csv")]

if not csv_paths:
    raise FileNotFoundError(
        f"Không tìm thấy file extracted_entities.csv"
    )

chunk_size = 5_000  
batch_size = 1_000  # Chia nhỏ batch để cập nhật progress thường xuyên hơn

# Resume / checkpoint settings
checkpoint_file = Path("checkpoint.json")
start_chunk_default = 1

# Ghi vào Neo4j
try:
    with driver.session() as session:
        for csv_path in csv_paths:
            # Chỉ sửa: resume_chunk tăng 1 để resume đúng
            try:
                resume_chunk = get_checkpoint(checkpoint_file).get(csv_path.name, 0) + 1
            except Exception:
                resume_chunk = start_chunk_default

            chunk_reader = pd.read_csv(csv_path, chunksize=chunk_size)
            
            for chunk_idx, chunk in enumerate(chunk_reader, start=1):
                if chunk_idx < resume_chunk:
                    tqdm.write(f"{csv_path.name} chunk {chunk_idx}: skipping (resume at {resume_chunk})")
                    continue
                
                # Xử lý dữ liệu từ chunk
                tqdm.write(f"\n{csv_path.name} chunk {chunk_idx}: Đang xử lý dữ liệu...")
                batch_rows = []
                
                for _, row in tqdm(
                    chunk.iterrows(),
                    total=len(chunk),
                    desc=f"Đọc chunk {chunk_idx}",
                    leave=False,
                    ncols=100
                ):
                    article_id_raw = row.get("article_id")
                    entity = normalize(row.get("entity", ""))
                    if pd.isna(article_id_raw) or not entity:
                        continue

                    batch_rows.append(
                        {
                            "article_id": int(article_id_raw),
                            "title": normalize(row.get("title", "")) or None,
                            "source": normalize(row.get("source", ""), "Unknown"),
                            "category": normalize(row.get("category", ""), "Unknown"),
                            "entity": entity,
                            "entity_type": normalize(row.get("entity_type", ""), "Unknown"),
                            "date": normalize(row.get("date", "")) or None,
                        }
                    )

                if not batch_rows:
                    save_checkpoint(checkpoint_file, csv_path.name, chunk_idx)
                    tqdm.write(f"{csv_path.name} chunk {chunk_idx}: Không có dữ liệu hợp lệ")
                    continue

                # Gửi records với thanh tiến trình
                tqdm.write(f"{csv_path.name} chunk {chunk_idx}: Gửi {len(batch_rows)} records vào Neo4j...")
                
                # Tạo progress bar cho việc gửi records
                with tqdm(
                    total=len(batch_rows),
                    desc=f"Gửi chunk {chunk_idx} vào Neo4j",
                    leave=True,
                    ncols=100,
                    unit="records"
                ) as pbar:
                    # Chia batch_rows thành các batch nhỏ hơn để cập nhật progress
                    for i in range(0, len(batch_rows), batch_size):
                        mini_batch = batch_rows[i:i + batch_size]
                        session.execute_write(write_batch, mini_batch, pbar)
                
                tqdm.write(f"{csv_path.name} chunk {chunk_idx}: ✓ Hoàn tất ({len(batch_rows)} records)")
                
                # Save checkpoint after successful write
                save_checkpoint(checkpoint_file, csv_path.name, chunk_idx)
except Exception as e:
    print("Error in create KG:", e)
finally:
    driver.close()
tqdm.write("\n✓ Hoàn tất tất cả các chunks!")