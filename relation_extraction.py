import pandas as pd
import re
from itertools import combinations
from neo4j import GraphDatabase
from tqdm import tqdm
import json
from google.genai import Client, types
from typing import Optional, Dict, List
import time
import os
import asyncio


# Track request count for each API key
current_api_index = 0  
request_count = 0  
lock = asyncio.Lock()

API_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")

async def get_api_key():
    """Returns an API key, switching every 3 requests for faster rotation"""
    global current_api_index, request_count
    
    async with lock:
        api_key = API_KEYS[current_api_index]
        request_count += 1  
        
        if request_count >= 3:  # Reduced from 5 to 3 for faster rotation
            current_api_index = (current_api_index + 1) % len(API_KEYS)
            request_count = 0  
    
    return api_key

# Cấu hình Gemini API
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# client = Client(api_key=GEMINI_API_KEY)

# Load relation patterns
with open("relation_vocab.json", "r", encoding="utf-8") as f:
    RELATION_PATTERNS = json.load(f)

# Tạo description cho từng loại relation
RELATION_DESCRIPTIONS = {
    "ORG_LOCATION": "Quan hệ về vị trí địa lý, nơi đặt trụ sở, hoạt động của tổ chức",
    "ADMINISTRATIVE_GOVERNANCE": "Quan hệ quản lý, chỉ đạo, điều hành, phê duyệt của cơ quan nhà nước",
    "ACTION_INTERACTION": "Các hành động, tương tác trực tiếp giữa các thực thể",
    "CAUSE_EFFECT": "Quan hệ nguyên nhân - kết quả, ảnh hưởng",
    "TEMPORAL": "Quan hệ về thời gian, mốc thời gian, khoảng thời gian",
    "SPATIAL": "Quan hệ không gian, phương hướng, vị trí tương đối",
    "ROLE_FUNCTION": "Vai trò, chức vụ, chức năng của người hoặc tổ chức",
    "EVENT_PARTICIPATION": "Tham gia, tổ chức, thực hiện sự kiện",
    "KNOWLEDGE_ACADEMIC": "Quan hệ học thuật, nghiên cứu, giáo dục",
    "COMPARISON_CONTRAST": "So sánh, đối chiếu giữa các thực thể",
    "PURPOSE_INTENT": "Mục đích, ý định của hành động",
    "METHOD_MANNER": "Phương thức, cách thức thực hiện",
    "PHYSICAL_TECHNICAL": "Quan hệ vật lý, kỹ thuật, cấu trúc",
    "COMMERCIAL_ECONOMIC": "Quan hệ thương mại, kinh tế, mua bán",
    "LEGAL": "Quan hệ pháp lý, luật pháp, vi phạm",
    "SOCIAL_PERSONAL": "Quan hệ xã hội, gia đình, cá nhân",
    "POSSESSION_ATTRIBUTE": "Sở hữu, thuộc tính, đặc điểm",
    "MEMBERSHIP": "Thành viên, thuộc về một nhóm/tổ chức",
    "REGULATION_COMPLIANCE": "Tuân thủ, quy định, quy chuẩn"
}

# Cache để tránh gọi API nhiều lần cho cùng câu
relation_cache: Dict[str, Optional[str]] = {}

def create_relation_prompt(sentence: str, entities: List[str]) -> str:
    """Tạo prompt cho LLM để phân loại relation"""
    
    # Tạo danh sách các relation types với mô tả
    relation_list = "\n".join([
        f"- {rel_type}: {desc}" 
        for rel_type, desc in RELATION_DESCRIPTIONS.items()
    ])
    
    entities_str = ", ".join(f'"{e}"' for e in entities)
    
    prompt = f"""Bạn là một chuyên gia phân tích quan hệ giữa các thực thể trong văn bản tiếng Việt.

    NHIỆM VỤ: Xác định loại quan hệ chính giữa các thực thể trong câu sau.

    CÂU CẦN PHÂN TÍCH:
    "{sentence}"

    CÁC THỰC THỂ TRONG CÂU:
    {entities_str}

    DANH SÁCH CÁC LOẠI QUAN HỆ HỢP LỆ:
    {relation_list}

    QUY TẮC:
    1. Chỉ được chọn MỘT loại quan hệ phù hợp nhất từ danh sách trên
    2. KHÔNG được tạo ra loại quan hệ mới ngoài danh sách
    3. Nếu không có quan hệ nào phù hợp, trả về "NONE"
    4. Chỉ trả về TÊN LOẠI QUAN HỆ (ví dụ: ORG_LOCATION), không giải thích thêm

    TRẢ LỜI (chỉ tên loại quan hệ):"""
    
    return prompt

async def detect_relation_with_llm(sentence: str, entities: List[str], max_retries: int = 3) -> Optional[str]:
    """
    Sử dụng Gemini để detect relation với ngữ nghĩa
    
    Args:
        sentence: Câu cần phân tích
        entities: Danh sách entities trong câu
        max_retries: Số lần thử lại nếu gặp lỗi
    
    Returns:
        Tên relation type hoặc None
    """
    # Kiểm tra cache
    cache_key = sentence.strip().lower()
    if cache_key in relation_cache:
        return relation_cache[cache_key]
    
    api_key = await get_api_key()
    client = Client(api_key=api_key)

    # Tạo prompt
    prompt = create_relation_prompt(sentence, entities)
    
    # Gọi LLM với retry logic
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    types.UserContent(
                        parts=[types.Part.from_text(text=prompt)]
                    )
                ],
                config=types.GenerateContentConfig(temperature=0.1, max_output_tokens=50)
            )
            
            # Lấy kết quả và chuẩn hóa
            print(response.text)
            if response.text is not None:
                result = response.text.strip().upper()
            else:
                result = "NONE"
            
            # Kiểm tra result có hợp lệ không
            if result == "NONE":
                relation_cache[cache_key] = None
                return None
            
            if result in RELATION_DESCRIPTIONS:
                relation_cache[cache_key] = result
                return result
            
            # Nếu LLM trả về không đúng format, thử parse
            for rel_type in RELATION_DESCRIPTIONS.keys():
                if rel_type in result:
                    relation_cache[cache_key] = rel_type
                    return rel_type
            
            # Nếu không match được, trả về None
            relation_cache[cache_key] = None
            return None
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Lỗi khi gọi API (thử lại {attempt + 1}/{max_retries}): {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"❌ Không thể gọi API sau {max_retries} lần thử: {e}")
                return None
    
    return None

async def detect_relation_hybrid(sentence: str, entities: List[str]) -> Optional[str]:
    """
    Phương pháp hybrid: Thử exact match trước, nếu không có thì dùng LLM
    Giúp tiết kiệm API calls và tăng tốc độ
    """
    # Thử exact match trước (nhanh và miễn phí)
    for rel, patterns in RELATION_PATTERNS.items():
        for p in patterns:
            if re.search(p, sentence.lower()):
                return rel
    
    # Nếu không match được, dùng LLM
    return await detect_relation_with_llm(sentence, entities)

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

async def main():
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

    print("Đang trích xuất quan hệ với LLM...")
    print("💡 Sử dụng phương pháp hybrid: exact match + LLM")

    for sent, group in tqdm(grouped, desc="Processing sentences"):
        entities = group["entity"].unique().tolist()
        
        if len(entities) < 2:
            continue
        
        # Sử dụng phương pháp hybrid
        rel = await detect_relation_hybrid(sent, entities)
        
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
    print(f"📊 Cache hits: {len(relation_cache)} câu đã được cache")

    # ============================================
    # GỬI QUAN HỆ VÀO NEO4J
    # ============================================

    try:
        # Kết nối Neo4j
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
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
    except Exception as e:
        print(f"❌ Lỗi khi ghi vào Neo4j: {e}")
    finally:
        driver.close()
        
    print("\n✓ Hoàn tất tất cả!")
    print(f"📈 Tổng số câu đã xử lý qua LLM: {len([v for v in relation_cache.values() if v is not None])}")


if __name__ == "__main__":
    asyncio.run(main())
