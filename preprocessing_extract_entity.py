import pandas as pd
import uuid
from underthesea import sent_tokenize, ner
from tqdm import tqdm
import re
from collections import Counter
import unicodedata
import psycopg2
import os

# ============================================
# CẤU HÌNH
# ============================================
MIN_ENTITY_LENGTH = 2
MIN_FREQUENCY = 2
INVALID_TYPES = {'O'}

# ============================================
# TẢI VIETNAMESE STOPWORDS
# ============================================
def load_stopwords(filepath='vietnamese_stopwords.txt'):
    """Đọc file stopwords tiếng Việt"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            stopwords = set(line.strip().lower() for line in f if line.strip())
        print(f"✔ Loaded {len(stopwords)} stopwords from {filepath}")
        return stopwords
    except FileNotFoundError:
        print(f"File {filepath} không tìm thấy")

STOPWORDS = load_stopwords()

# ============================================
# TIỀN XỬ LÝ VĂN BẢN
# ============================================
def remove_special_chars(text):
    """Xóa ký tự đặc biệt, giữ lại chữ cái, số và khoảng trắng"""
    # Giữ lại chữ cái (bao gồm tiếng Việt), số, khoảng trắng và một số dấu câu cần thiết
    text = re.sub(r'[^\w\s\.,;:\-\(\)]', ' ', text, flags=re.UNICODE)
    return text

def normalize_whitespace(text):
    """Chuẩn hóa khoảng trắng"""
    # Thay thế nhiều khoảng trắng bằng 1 khoảng trắng
    text = re.sub(r'\s+', ' ', text)
    # Xóa khoảng trắng ở đầu và cuối
    text = text.strip()
    # Xóa khoảng trắng trước dấu câu
    text = re.sub(r'\s+([.,;:!?])', r'\1', text)
    return text

def normalize_unicode(text):
    """Chuẩn hóa Unicode (NFC) để xử lý các ký tự tiếng Việt"""
    return unicodedata.normalize('NFC', text)

def remove_urls(text):
    """Xóa URLs"""
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    text = re.sub(r'www\.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    return text

def remove_emails(text):
    """Xóa email addresses"""
    return re.sub(r'\S+@\S+', '', text)

def remove_phone_numbers(text):
    """Xóa số điện thoại"""
    text = re.sub(r'(\+84|0)[0-9]{9,10}', '', text)
    return text

def remove_numbers_only(text):
    """Xóa các chuỗi chỉ chứa số"""
    text = re.sub(r'\b\d+\b', '', text)
    return text

def remove_repeated_chars(text):
    """Xóa ký tự lặp lại (vd: 'rấtttt' -> 'rất')"""
    return re.sub(r'(.)\1{2,}', r'\1\1', text)

def clean_text(text):
    """Pipeline làm sạch text toàn diện"""
    if pd.isna(text):
        return ""
    
    text = str(text)
    
    # 1. Chuẩn hóa Unicode
    text = normalize_unicode(text)
    
    # 2. Chuyển thành chữ thường (tùy chọn - có thể giữ chữ hoa cho entities)
    # text = text.lower()
    
    # 3. Xóa URLs và emails
    text = remove_urls(text)
    text = remove_emails(text)
    
    # 4. Xóa số điện thoại
    text = remove_phone_numbers(text)
    
    # 5. Xóa ký tự lặp
    text = remove_repeated_chars(text)
    
    # 6. Xóa ký tự đặc biệt
    text = remove_special_chars(text)
    
    # 7. Xóa các số đứng một mình (không phải trong từ)
    text = remove_numbers_only(text)
    
    # 8. Chuẩn hóa khoảng trắng
    text = normalize_whitespace(text)
    
    # 9. Thay thế newline bằng khoảng trắng
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    
    # 10. Chuẩn hóa lại khoảng trắng lần cuối
    text = normalize_whitespace(text)
    
    return text

# ============================================
# XỬ LÝ ENTITIES
# ============================================
def normalize_entity(entity):
    """Chuẩn hóa entity name"""
    # Chuẩn hóa Unicode
    entity = normalize_unicode(entity)
    
    # Loại bỏ khoảng trắng thừa
    entity = normalize_whitespace(entity)
    
    # Loại bỏ ký tự đặc biệt ở đầu/cuối
    entity = re.sub(r'^[^\w]+|[^\w]+$', '', entity, flags=re.UNICODE)
    
    # Xóa dấu ngoặc đơn, ngoặc kép thừa
    entity = entity.strip('()[]{}"\'""`')
    
    return entity

def is_stopword_entity(entity):
    """Kiểm tra entity có phải là stopword không"""
    words = entity.lower().split()
    # Loại bỏ nếu tất cả các từ đều là stopwords
    return all(word in STOPWORDS for word in words)

def is_valid_entity(entity, entity_type):
    """Kiểm tra entity có hợp lệ không - CẢI TIẾN"""
    entity_clean = entity.lower().strip()
    
    # 1. Loại bỏ entities quá ngắn
    if len(entity_clean) < MIN_ENTITY_LENGTH:
        return False
    
    # 2. Loại bỏ entities chỉ là stopwords
    if is_stopword_entity(entity_clean):
        return False
    
    # 3. Loại bỏ entities chỉ chứa số
    if entity_clean.replace(' ', '').isdigit():
        return False
    
    # 4. Loại bỏ entities chỉ chứa ký tự đặc biệt
    if not re.search(r'[a-zA-ZÀ-ỹ]', entity, re.UNICODE):
        return False
    
    # 5. Loại bỏ tag không hợp lệ
    if entity_type in INVALID_TYPES:
        return False
    
    # 6. Loại bỏ entities chỉ chứa 1 ký tự lặp lại
    if len(set(entity_clean.replace(' ', ''))) == 1:
        return False
    
    # 7. Loại bỏ entities bắt đầu hoặc kết thúc bằng stopword
    words = entity_clean.split()
    if len(words) > 1:
        if words[0] in STOPWORDS or words[-1] in STOPWORDS:
            return False
    
    # 8. Loại bỏ entities quá dài (có thể là lỗi)
    if len(entity_clean) > 100:
        return False
    
    return True

def merge_consecutive_entities(ner_results):
    """Gộp các entity liên tiếp cùng loại - HỖ TRỢ 4-TUPLE"""
    merged = []
    current_words = []
    current_type = None
    
    for item in ner_results:
        word = None
        tag = 'O'
        
        # underthesea trả về tuple (word, pos_tag, chunk_tag, ner_tag)
        if isinstance(item, tuple):
            if len(item) == 4:
                word, pos_tag, chunk_tag, tag = item
            elif len(item) == 3:
                word, pos_tag, tag = item
            elif len(item) == 2:
                word, tag = item
            else:
                continue
        elif isinstance(item, dict):
            word = item.get('word', '')
            tag = item.get('entity', item.get('ner', 'O'))
        else:
            continue
        
        if not word:
            continue
        
        # Xử lý B-tag (Beginning) và I-tag (Inside)
        if tag.startswith('B-'):
            # Lưu entity trước đó (nếu có)
            if current_words and current_type:
                merged.append((' '.join(current_words), current_type))
            # Bắt đầu entity mới
            current_words = [word]
            current_type = tag[2:]  # Bỏ "B-"
            
        elif tag.startswith('I-') and current_type == tag[2:]:
            # Tiếp tục entity hiện tại
            current_words.append(word)
            
        else:
            # Kết thúc entity (nếu có)
            if current_words and current_type:
                merged.append((' '.join(current_words), current_type))
            current_words = []
            current_type = None
    
    # Lưu entity cuối cùng
    if current_words and current_type:
        merged.append((' '.join(current_words), current_type))
    
    return merged

def extract_entities_by_pos_pattern(ner_results):
    """
    Fallback: Trích xuất entities dựa vào POS patterns
    Ví dụ: N (noun) liên tiếp có thể là tên riêng/tổ chức
    """
    entities = []
    current_words = []
    current_pos = []
    
    for item in ner_results:
        if isinstance(item, tuple) and len(item) >= 2:
            word = item[0]
            pos_tag = item[1]
            
            # Nhóm các danh từ (N), danh từ riêng (Np), số (M) liên tiếp
            if pos_tag in ['N', 'Np', 'M', 'Ny']:
                current_words.append(word)
                current_pos.append(pos_tag)
            else:
                # Kết thúc chuỗi
                if len(current_words) >= 2:  # Ít nhất 2 từ
                    entity = ' '.join(current_words)
                    # Xác định type dựa vào POS
                    if 'Np' in current_pos:
                        entity_type = 'PER'  # Person
                    elif 'M' in current_pos or 'Ny' in current_pos:
                        entity_type = 'MISC'  # Miscellaneous
                    else:
                        entity_type = 'ORG'  # Organization
                    
                    entities.append((entity, entity_type))
                
                current_words = []
                current_pos = []
    
    # Lưu chuỗi cuối cùng
    if len(current_words) >= 2:
        entity = ' '.join(current_words)
        entity_type = 'PER' if 'Np' in current_pos else 'ORG'
        entities.append((entity, entity_type))
    
    return entities

def extract_entities_by_capitalization(sentence):
    """
    Fallback: Trích xuất entities dựa vào viết hoa
    (Trong tiếng Việt ít hiệu quả nhưng vẫn có giá trị)
    """
    entities = []
    # Tìm chuỗi từ viết hoa liên tiếp
    pattern = r'\b([A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]*(?:\s+[A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]*)+)\b'
    matches = re.finditer(pattern, sentence)
    
    for match in matches:
        entity = match.group(1)
        if len(entity.split()) >= 2:  # Ít nhất 2 từ
            entities.append((entity, 'PER'))
    
    return entities

def deduplicate_similar_entities(entities_list):
    """Loại bỏ các entities tương tự nhau (chỉ khác chữ hoa/thường)"""
    seen = {}
    unique_entities = []
    
    for entity_dict in entities_list:
        entity_lower = entity_dict['entity'].lower()
        
        if entity_lower not in seen:
            seen[entity_lower] = entity_dict
            unique_entities.append(entity_dict)
        else:
            # Giữ lại entity có chữ hoa (ưu tiên tên riêng)
            if entity_dict['entity'][0].isupper() and not seen[entity_lower]['entity'][0].isupper():
                seen[entity_lower] = entity_dict
                # Cập nhật trong list
                for i, e in enumerate(unique_entities):
                    if e['entity'].lower() == entity_lower:
                        unique_entities[i] = entity_dict
                        break
    
    return unique_entities

# ============================================
# TRÍCH XUẤT ENTITIES
# ============================================
def extract_entities_from_sentences(sentences, debug=False, use_fallback=True):
    """Trích xuất entities với validation và normalization + FALLBACK methods"""
    entities = []
    debug_info = {
        'total_ner_results': 0,
        'merged_entities': 0,
        'pos_pattern_entities': 0,
        'capitalization_entities': 0,
        'valid_entities': 0,
        'filtered_reasons': Counter()
    }
    
    for sentence in sentences:
        if not sentence or len(sentence.strip()) < 10:
            continue
            
        try:
            # METHOD 1: NER với underthesea
            ner_results = ner(sentence)
            debug_info['total_ner_results'] += len(ner_results)
            
            # Debug: In ra format của ner_results
            if debug and debug_info['total_ner_results'] <= 5:
                print(f"DEBUG - NER result format: {type(ner_results[0]) if ner_results else 'empty'}")
                if ner_results:
                    print(f"DEBUG - Sample: {ner_results[:3]}")
            
            # Gộp entities từ NER tags
            merged_entities = merge_consecutive_entities(ner_results)
            debug_info['merged_entities'] += len(merged_entities)
            
            # METHOD 2: Fallback - POS pattern extraction
            if use_fallback and len(merged_entities) == 0:
                pos_entities = extract_entities_by_pos_pattern(ner_results)
                debug_info['pos_pattern_entities'] += len(pos_entities)
                merged_entities.extend(pos_entities)
            
            # METHOD 3: Fallback - Capitalization pattern
            if use_fallback and len(merged_entities) < 2:
                cap_entities = extract_entities_by_capitalization(sentence)
                debug_info['capitalization_entities'] += len(cap_entities)
                merged_entities.extend(cap_entities)
            
            for entity, entity_type in merged_entities:
                # Chuẩn hóa entity
                entity_normalized = normalize_entity(entity)
                
                # Debug validation
                if not entity_normalized:
                    debug_info['filtered_reasons']['empty_after_normalization'] += 1
                    continue
                
                if len(entity_normalized) < MIN_ENTITY_LENGTH:
                    debug_info['filtered_reasons']['too_short'] += 1
                    continue
                
                if is_stopword_entity(entity_normalized):
                    debug_info['filtered_reasons']['stopword'] += 1
                    continue
                
                if entity_type in INVALID_TYPES:
                    debug_info['filtered_reasons']['invalid_type'] += 1
                    continue
                
                # Validate
                if is_valid_entity(entity_normalized, entity_type):
                    entities.append({
                        "entity": entity_normalized,
                        "entity_type": entity_type,
                        "sentence": sentence.strip()
                    })
                    debug_info['valid_entities'] += 1
                else:
                    debug_info['filtered_reasons']['other_validation_failed'] += 1
                    
        except Exception as e:
            if debug:
                print(f"⚠ Error processing sentence: {str(e)}")
            continue
    
    # Deduplicate similar entities
    entities = deduplicate_similar_entities(entities)
    
    if debug:
        print("\nDEBUG INFO:")
        print(f"  - Total NER results: {debug_info['total_ner_results']}")
        print(f"  - NER merged entities: {debug_info['merged_entities']}")
        print(f"  - POS pattern entities: {debug_info['pos_pattern_entities']}")
        print(f"  - Capitalization entities: {debug_info['capitalization_entities']}")
        print(f"  - Valid entities: {debug_info['valid_entities']}")
        print(f"  - Filtered reasons: {dict(debug_info['filtered_reasons'])}")
    
    return entities

# ============================================
# XỬ LÝ TOÀN BỘ DATASET
# ============================================
def process_all(df, output_csv):
    """Xử lý toàn bộ DataFrame với preprocessing đầy đủ"""
    
    # ============================================
    # 1. TIỀN XỬ LÝ DỮ LIỆU
    # ============================================
    print("\nBước 1: Tiền xử lý dữ liệu...")
    
    # # Thêm ID nếu chưa có
    # if "id" not in df.columns:
    #     df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    #     print("✔ Added 'id' column using UUID4")
    # else:
    #     print("✔ 'id' column exists")
    
    # # Chuẩn hóa tên cột
    # df.columns = df.columns.str.lower()
    # print("✔ Chuẩn hóa tên cột:", df.columns.tolist())
    
    # # Xóa dòng trống
    # df = df.dropna(subset=['content']).copy()  # Thêm .copy() để tránh warning
    # print(f"✔ Số bài viết sau khi xóa dòng trống: {len(df)}")
    
    # # Làm sạch text
    # print("✔ Đang làm sạch text...")
    # df["content_clean"] = df["content"].apply(clean_text)
    # df["title_clean"] = df["title"].apply(clean_text)
    
    # # Chia câu
    # print("✔ Đang chia câu...")
    # df["sentences"] = df["content_clean"].apply(lambda x: sent_tokenize(x) if x else [])
    
    # Thống kê sau tiền xử lý
    total_sentences = df["sentences"].apply(len).sum()
    print(f"✔ Tổng số câu: {total_sentences}")
    
    # ============================================
    # 2. TRÍCH XUẤT ENTITIES
    # ============================================
    print("\nBước 2: Trích xuất entities...")
    all_entities = []
    
    # Debug cho bài đầu tiên
    print("\nDEBUG: Testing first article...")
    first_row = df.iloc[0]
    test_entities = extract_entities_from_sentences(first_row["sentences"], debug=True)
    print(f"✔ Test extracted {len(test_entities)} entities from first article")
    
    # Nếu vẫn không có entities, thử với raw text
    if len(test_entities) == 0:
        print("\n⚠ No entities from cleaned text. Testing with raw text...")
        raw_sentences = sent_tokenize(first_row["content"])[:3]  # Test 3 câu đầu
        for i, sent in enumerate(raw_sentences):
            print(f"\n--- Sentence {i+1} ---")
            print(f"Text: {sent[:100]}...")
            ner_result = ner(sent)
            print(f"NER result: {ner_result[:5] if len(ner_result) > 5 else ner_result}")
    
    print("\nProcessing all articles...")
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing articles"):
        entities = extract_entities_from_sentences(row["sentences"])
        
        for e in entities:
            all_entities.append({
                "article_id": row["id"],
                "entity": e["entity"],
                "entity_type": e["entity_type"],
                "sentence": e["sentence"],
                "source": row.get("source", ""),
                "category": row.get("category", ""),
                "date": row.get("time", "")
            })
    
    # Chuyển sang DataFrame
    result_df = pd.DataFrame(all_entities)
    
    if len(result_df) == 0:
        print("⚠ No entities extracted!")
        return
    
    # ============================================
    # 3. POST-PROCESSING
    # ============================================
    print("\nBước 3: Post-processing...")
    
    # Loại bỏ duplicates trong cùng 1 bài
    result_df = result_df.drop_duplicates(
        subset=["article_id", "entity", "entity_type"], 
        keep="first"
    )
    
    # Thống kê
    entity_counts = result_df["entity"].value_counts()
    entity_type_counts = result_df["entity_type"].value_counts()
    
    print(f"\n✔ Tổng entities: {len(result_df)}")
    print(f"✔ Unique entities: {len(entity_counts)}")
    print(f"\nTop 20 entities xuất hiện nhiều nhất:")
    print(entity_counts.head(20))
    print(f"\nPhân bố theo entity type:")
    print(entity_type_counts)
    
    # Lọc entities xuất hiện quá ít
    valid_entities = entity_counts[entity_counts >= MIN_FREQUENCY].index
    result_df_filtered = result_df[result_df["entity"].isin(valid_entities)]
    
    print(f"\n✔ Sau khi lọc (min_freq={MIN_FREQUENCY}): {len(result_df_filtered)} entities")
    
    # ============================================
    # 4. LƯU FILE
    # ============================================
    print("\nBước 4: Lưu kết quả...")
    
    # Lưu file chính
    result_df_filtered.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    print(f"\nHOÀN THÀNH!")


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

# Áp dụng cho title + content (giờ dùng lowercase)
df["content_clean"] = df["content"].apply(clean_text)
df["title_clean"] = df["title"].apply(clean_text)

# Tách câu từ content
df["sentences"] = df["content_clean"].apply(lambda x: sent_tokenize(x))


# ============================================
# CHẠY PIPELINE
# ============================================
if __name__ == "__main__":
    # # Tạo kết nối đến pgAdmin
    # conn = psycopg2.connect(
    #     host=os.getenv('DB_HOST'),
    #     database=os.getenv('DB_NAME'),
    #     user=os.getenv('DB_USER'),
    #     password=os.getenv('DB_PASSWORD'),
    #     port=os.getenv('DB_PORT')
    # )
    # query = """
    #     SELECT id, title, content, time, source, category
    #     FROM paper
    #     WHERE time >= '2025-12-01'
    # """
    # df = pd.read_sql(query, conn)
    print(f"✔ Đọc được {len(df)} bài viết")
    # conn.close()
    
    # Chạy pipeline
    process_all(df, "extracted_entities.csv")
