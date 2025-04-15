import psycopg2
from psycopg2 import Error
import csv
import re
import pandas as pd
from pyvi import ViTokenizer

db_params = {
    "dbname": "PBL7",
    "user": "avnadmin",
    "password": "AVNS_YTPX-cSc4J5wj3wEVLv",
    "host": "pg-b86005-nodejs-tutorial.c.aivencloud.com",
    "port": "18400",
    "sslmode": "require"
}

def preprocess_and_save(csv_file_path):
    df = pd.read_csv(csv_file_path)
    df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M GMT+7')
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
    paper_dataset = ['tuoitre', 'vnexpress', 'znews']
    for paper in paper_dataset:
        csv_file_path = f"dataset_paper_{paper}.csv"
        preprocess_and_save(csv_file_path)