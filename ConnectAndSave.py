import psycopg2
from psycopg2 import Error
import csv
import os
import re
import pandas as pd
from pyvi import ViTokenizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from collections import Counter
from datetime import datetime, timedelta
import pytz
import time

db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

def clean_text(text):
    if not isinstance(text, str):
        return ''
    text = re.sub(r'\s+', ' ', text.strip())
    return text

def preprocess_and_save(csv_file_path, paper):
    if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
        print(f"File {csv_file_path} không tồn tại hoặc rỗng. Bỏ qua.")
        return

    try:
        df = pd.read_csv(csv_file_path)
    except pd.errors.EmptyDataError:
        print(f"File {csv_file_path} không chứa dữ liệu hợp lệ (EmptyDataError). Bỏ qua.")
        return
    except Exception as e:
        print(f"Lỗi không xác định khi đọc file {csv_file_path}: {e}")
        return
    
    # Làm sạch khoảng trắng trong cột Title và Content
    df['Title'] = df['Title'].apply(clean_text)
    df['Content'] = df['Content'].apply(clean_text)
    
    if paper == 'tuoitre':
        df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M GMT+7', errors='coerce')
    else:
        # Loại bỏ tên ngày và dấu phẩy đầu tiên
        df['Time'] = df['Time'].str.replace(r'^.*?,\s*', '', regex=True)
        # Loại bỏ (GMT+7) và dấu phẩy trước giờ
        df['Time'] = df['Time'].str.replace(r',\s*(?=\d{1,2}:\d{2}\s*\(GMT\+7\))', ' ', regex=True)
        df['Time'] = df['Time'].str.replace(r'\s*\(GMT\+7\)', '', regex=True)
        # Chuẩn hóa ngày: 13/4/2025 -> 13/04/2025
        df['Time'] = df['Time'].str.replace(r'(\d+)/(\d+)/(\d+)', r'\1/0\2/\3', regex=True)
        df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M', errors='coerce')
    
    invalid_rows = df['Time'].isna().sum()
    if invalid_rows > 0:
        print(f"Cảnh báo: {invalid_rows} bản ghi trong {csv_file_path} có thời gian không hợp lệ, sẽ bị bỏ qua.")
        df = df.dropna(subset=['Time'])

    df['Year'] = df['Time'].dt.year
    df['Month'] = df['Time'].dt.month
    df['Day'] = df['Time'].dt.day

    df['Text'] = df['Title'] + ' ' + df['Content']
    try:
        with open('vietnamese_stopwords.txt', mode='r', encoding='utf-8') as file:
            stop_words = set(line.strip() for line in file)
    except FileNotFoundError:
        print("Không tìm thấy file stopword.")
        stop_words = set()

    def preprocess_text(text):
        text = re.sub(r'[^\w\s]', '', str(text).lower())
        tokens = ViTokenizer.tokenize(text)
        return ' '.join([word for word in tokens.split() if word not in stop_words])

    df['Tokens'] = df['Text'].apply(preprocess_text)

    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO paper (source, url, category, keyword, time, title, content, tokens)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        records = list(zip(
            df['Source'],
            df['URL'],
            df['Category'],
            df['Keyword'],
            df['Time'],
            df['Title'],
            df['Content'],
            df['Tokens']
        ))

        cursor.executemany(insert_query, records)
        connection.commit()
        print(f"Lưu {cursor.rowcount} bản ghi từ {csv_file_path} thành công!")

    except (Exception, Error) as error:
        print("Lỗi khi lưu dữ liệu:", error)
        if connection:
            connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Hàm để lấy category và keyword phổ biến nhất trong mỗi chủ đề
def analyze_category_keyword(df):
    topic_info = {}
    for topic_idx in df['Topic'].unique():
        # Lấy các bài báo thuộc chủ đề hiện tại
        topic_df = df[df['Topic'] == topic_idx]
        
        # Tìm category phổ biến nhất
        category_counts = topic_df['category'].value_counts()
        top_category = category_counts.index[0] if not category_counts.empty else "Unknown"
        
        # Tìm keyword phổ biến nhất
        all_keywords = []
        for keywords in topic_df['keyword']:
            if isinstance(keywords, str):
                all_keywords.extend([kw.strip().lower() for kw in keywords.split(',')])
        keyword_counts = Counter(all_keywords)
        # Lấy top 3 keyword phổ biến
        top_keywords = [kw for kw, count in keyword_counts.most_common(3)]
        
        topic_info[topic_idx] = {
            'top_category': top_category,
            'top_keywords': top_keywords
        }
    
    return topic_info


def run_lda_model():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        timezone = pytz.timezone("Asia/Ho_Chi_Minh")
        yesterday = datetime.now(timezone) - timedelta(days=1)
        # formatted_date = yesterday.strftime("%Y-%m-%d")  # Định dạng thành 'YYYY-MM-DD'
        month = yesterday.month
        year = yesterday.year
        formatted_date = f"{year}-{month:02d}"  # Định dạng thành 'YYYY-MM'
        print(formatted_date)

        # Câu truy vấn SQL với parameterized query
        sql_query = """
            SELECT * FROM paper WHERE time LIKE %s
        """

        # Tạo pattern cho LIKE
        date_pattern = f"{formatted_date}%"

        df = pd.read_sql(sql_query, connection, params=(date_pattern,))
        
        #=======================
        # df = pd.read_sql(sql_query, connection, params=(date_pattern,), dtype={'tokens': str, 'content': str, 'title': str})
        #=======================
        
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S')
        df['year'] = df['time'].dt.year
        df['month'] = df['time'].dt.month
        df['day'] = df['time'].dt.day
        df.loc[df['keyword'].isin(['NaN', 'Null']), 'keyword'] = df['category']

        # Vector hóa văn bản
        vectorizer = CountVectorizer(max_features=2000)
        X = vectorizer.fit_transform(df['tokens'])

        # Mô hình LDA
        num_topics = 20
        lda = LatentDirichletAllocation(n_components=num_topics, random_state=42)
        lda_output = lda.fit_transform(X)

        # Gán chủ đề cho mỗi bài báo
        df['Topic'] = lda_output.argmax(axis=1)

        # Phân tích category và keyword
        topic_info = analyze_category_keyword(df)
        
        # Tạo từ điển nhãn mới
        topic_names_manual = {}
        for topic_idx in topic_info.keys():
            # Lấy thông tin category và keyword
            top_category = topic_info[topic_idx]['top_category']
            top_keywords = topic_info[topic_idx]['top_keywords']
            
            # Tạo nhãn mới dựa trên category và keyword
            # Ví dụ: lấy category và 1-2 keyword phổ biến
            if top_keywords:
                label = f"{top_category.lower()} {'_'.join(top_keywords[:2]).lower()}"
            else:
                label = top_category.lower()
                
            topic_names_manual[topic_idx] = label.replace(' ', '_')
        
        # Gán nhãn mới vào DataFrame
        df['Topic_Name'] = df['Topic'].map(topic_names_manual)
        
        update_query = """
            UPDATE paper
            SET topic_name = %s
            WHERE id = %s
        """

        records = list(zip(
            df['Topic_Name'],
            df['id']
        ))

        cursor.executemany(update_query, records)
        connection.commit()
        print("Cập nhật topic name thành công")

    except (Exception, Error) as error:
        print("Lỗi khi lưu dữ liệu:", error)
        if connection:
            connection.rollback()
    
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    paper_dataset = ['tuoitre', 'vnexpress', 'znews']
    for paper in paper_dataset:
        csv_file_path = f"dataset_paper_{paper}.csv"
        preprocess_and_save(csv_file_path, paper)
    
    time.sleep(5)
    run_lda_model()