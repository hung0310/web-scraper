import os
import time
import csv
import random
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pytz

vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_time = datetime.now(vn_timezone)

# Xác định khung giờ cố định dựa trên giờ hiện tại
# Chia ngày thành các khung 3 tiếng: 0-2, 3-5, 6-8, 9-11, 12-14, 15-17, 18-20, 21-23
current_hour = current_time.hour
time_slot_start_hour = (current_hour // 3) * 3  # Làm tròn xuống bội số của 3

# Tạo khung giờ: từ X:00:00 đến X+2:59:59
time_start = current_time.replace(hour=time_slot_start_hour, minute=0, second=0, microsecond=0)
time_end = time_start.replace(hour=time_slot_start_hour + 2, minute=59, second=59, microsecond=999999)

print(f"Khung giờ crawl: {time_start.strftime('%Y-%m-%d %H:%M:%S')} đến {time_end.strftime('%Y-%m-%d %H:%M:%S')}")

csv_file = 'dataset_paper_tuoitre.csv'
base_url = 'https://tuoitre.vn'

# 🛠 Hàm khởi tạo driver
def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--dns-prefetch-disable") 
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    return driver

# 🛠 Hàm chờ phần tử
def wait_for_element(driver, by, value, timeout=10):
    try:
        return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        return None

# 🛠 Hàm đọc các URL đã crawl từ file CSV
def load_crawled_urls(csv_file):
    crawled_urls = set()
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            next(reader, None)  # Bỏ qua header
            for row in reader:
                if len(row) >= 2:
                    crawled_urls.add(row[1])  # URL bài báo ở cột thứ 2
    except FileNotFoundError:
        pass
    return crawled_urls

# 🛠 Hàm parse thời gian từ text Tuổi Trẻ
def parse_tuoitre_time(time_text):
    """
    Parse thời gian từ Tuổi Trẻ format:
    - "23/11/2024 02:30 GMT+7"
    - "Thứ bảy, 23/11/2024 02:30 GMT+7"
    """
    try:
        time_text = time_text.strip()
        
        # Format: "23/11/2024 02:30 GMT+7" hoặc "Thứ bảy, 23/11/2024 02:30 GMT+7"
        match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})', time_text)
        if match:
            day, month, year, hour, minute = match.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), tzinfo=vn_timezone)
        
        print(f"Không parse được thời gian: {time_text}")
        return None
        
    except Exception as e:
        print(f"Lỗi khi parse thời gian '{time_text}': {e}")
        return None

# 🛠 Hàm kiểm tra bài viết có trong khung giờ không
def is_in_time_range(article_time):
    """Kiểm tra xem thời gian bài viết có nằm trong khung giờ [time_start, time_end] không"""
    if not article_time:
        return False
    return time_start <= article_time <= time_end

# 🛠 Hàm crawl bài báo
def crawl_article(driver, category_name, article_href, writer, crawled_urls):
    if article_href in crawled_urls:
        print(f"Bài {article_href} đã được crawl, bỏ qua.")
        return True

    for attempt in range(3):
        try:
            driver.get(article_href)
            wait_for_element(driver, By.CLASS_NAME, "detail-content", timeout=10)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            time_elem = soup.select_one('div.detail-time > div')
            time_paper = time_elem.get_text(strip=True) if time_elem else "N/A"

            # Kiểm tra thời gian bài viết
            article_time = parse_tuoitre_time(time_paper)
            if not is_in_time_range(article_time):
                print(f"Bài viết {article_href} không trong khung giờ, bỏ qua.")
                return True  # Return True để không retry

            title_elem = soup.select_one('h1.detail-title')
            title_paper = title_elem.get_text(strip=True) if title_elem else "Không có tiêu đề"

            content_elems = soup.select('div.detail-content p')
            content_paper = " ".join([p.get_text(strip=True) for p in content_elems if p])

            keyword_elems = soup.select('div.detail-tab > a')
            keyword_paper = ",".join([a.get_text(strip=True) for a in keyword_elems if a])

            writer.writerow(["Tuoi tre", article_href, category_name, keyword_paper, time_paper, title_paper, content_paper])
            crawled_urls.add(article_href)
            print(f"Đã crawl bài {article_href} - Thời gian: {time_paper}")
            return True
        except TimeoutException:
            print(f"Timeout khi tải {article_href}, thử lại {attempt+1}/3")
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            print(f"Lỗi khi tải {article_href}: {e}")
            return False
    return False

# 🏁 Bắt đầu quá trình crawl
driver = init_driver()
crawled_urls = load_crawled_urls(csv_file)

file_mode = 'w'
write_header = True

try:
    driver.get(base_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    categories = soup.select('ul.menu-nav > li > a')

    with open(csv_file, mode=file_mode, encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(["Source", "URL", "Category", "Keyword", "Time", "Title", "Content"])
        
        for cat in categories:
            category_url = f"{base_url}{cat['href']}"
            category_name = cat.get_text(strip=True)

            print(f"Đang xử lý danh mục: {category_name}")
            driver.get(category_url)
            time.sleep(2)

            article_hrefs = set()
            last_height = driver.execute_script("return document.body.scrollHeight")
            stop_scroll = False

            while not stop_scroll:
                soup_articles = BeautifulSoup(driver.page_source, 'html.parser')
                articles = soup_articles.select('div.box-category-item > a')
                
                for article in articles:
                    article_href = f"{base_url}{article['href']}"
                    date_str = article['href'].split("-")[-1][:8]

                    try:
                        article_date = datetime.strptime(date_str, "%Y%m%d").date()
                        
                        # Chỉ lấy bài trong ngày hiện tại
                        if article_date == current_time.date():
                            article_hrefs.add(article_href)
                        elif article_date < current_time.date():
                            print(f"Dừng scroll trong {category_name}, phát hiện bài cũ.")
                            stop_scroll = True
                            break
                    except ValueError:
                        continue

                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            print(f"Tìm thấy {len(article_hrefs)} bài trong {category_name}")

            for article_href in article_hrefs:
                if not crawl_article(driver, category_name, article_href, writer, crawled_urls):
                    print("Khởi động lại driver do lỗi nghiêm trọng.")
                    driver.quit()
                    driver = init_driver()
                    continue

except Exception as e:
    print(f"Lỗi chính: {e}")
finally:
    driver.quit()

print("Hoàn tất quá trình thu thập dữ liệu.")
