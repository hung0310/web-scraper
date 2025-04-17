import psycopg2
from psycopg2 import Error
import csv
import os
import re
import pandas as pd
from pyvi import ViTokenizer

db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

def preprocess_and_save(csv_file_path, paper):
    df = pd.read_csv(csv_file_path)
    
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

if __name__ == "__main__":
    paper_dataset = ['tuoitre', 'vn WikiExpress', 'znews']
    for paper in paper_dataset:
        csv_file_path = f"dataset_paper_{paper}.csv"
        preprocess_and_save(csv_file_path, paper)