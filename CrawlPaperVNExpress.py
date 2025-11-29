from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
import time
import csv
import random
import pytz
import re

base_url = 'https://vnexpress.net'
csv_file = 'dataset_paper_vnexpress.csv'

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
    driver.set_page_load_timeout(180)
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
            next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    crawled_urls.add(row[1])
    except FileNotFoundError:
        pass
    return crawled_urls

# 🛠 Hàm parse thời gian từ text VNExpress
def parse_vnexpress_time(time_text):
    """
    Parse thời gian từ VNExpress format:
    - "Thứ bảy, 23/11/2024, 02:30 (GMT+7)"
    - "Hôm qua, 02:30"
    - "2 giờ trước"
    - "30 phút trước"
    """
    try:
        time_text = time_text.strip()
        
        # Format đầy đủ: "Thứ bảy, 23/11/2024, 02:30 (GMT+7)"
        match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4}),?\s*(\d{1,2}):(\d{2})', time_text)
        if match:
            day, month, year, hour, minute = match.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), tzinfo=vn_timezone)
        
        # "Hôm qua, HH:MM"
        if "Hôm qua" in time_text or "hôm qua" in time_text:
            match = re.search(r'(\d{1,2}):(\d{2})', time_text)
            if match:
                hour, minute = match.groups()
                yesterday = current_time - timedelta(days=1)
                return yesterday.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
        
        # "X giờ trước"
        match = re.search(r'(\d+)\s*giờ trước', time_text)
        if match:
            hours_ago = int(match.group(1))
            return current_time - timedelta(hours=hours_ago)
        
        # "X phút trước"
        match = re.search(r'(\d+)\s*phút trước', time_text)
        if match:
            minutes_ago = int(match.group(1))
            return current_time - timedelta(minutes=minutes_ago)
        
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
def crawl_article(driver, article_url, category_name, writer, crawled_urls):
    if article_url in crawled_urls:
        print(f"Bài {article_url} đã được crawl, bỏ qua.")
        return True

    for attempt in range(3):
        try:
            driver.get(article_url)
            time.sleep(2)
            
            soup_detail_article = BeautifulSoup(driver.page_source, 'html.parser')
            
            keyword_elems = driver.find_elements(By.CLASS_NAME, 'item-tag')
            
            time_article = soup_detail_article.select_one('div.sidebar-1 > div.header-content > span.date, span.date')
            title_article = soup_detail_article.select_one('div.sidebar-1 > h1.title-detail, h1.title-detail')
            para_head_article = soup_detail_article.select_one('div.sidebar-1 > p.description, p.description')
            para_main_article = soup_detail_article.select('div.sidebar-1 > article.fck_detail > p.Normal, article.fck_detail > p.Normal, p.Normal')

            time_text = time_article.get_text(strip=True) if time_article else 'N/A'
            title_text = title_article.get_text(strip=True) if title_article else 'N/A'
            para_head_text = para_head_article.get_text(strip=True) if para_head_article else ''
            para_main_text = " ".join([p.get_text(strip=True) for p in para_main_article]) if para_main_article else ''
            keyword_paper = ",".join([a.text for a in keyword_elems])

            full_content = f"{para_head_text} {para_main_text}".strip()

            if not full_content:
                print(f"Không tìm thấy nội dung cho bài viết: {article_url}")
                return False

            # Kiểm tra thời gian bài viết
            article_time = parse_vnexpress_time(time_text)
            if not is_in_time_range(article_time):
                print(f"Bài viết {article_url} không trong khung giờ, bỏ qua.")
                return True  # Return True để không retry

            writer.writerow(["VN Express", article_url, category_name, keyword_paper, time_text, title_text, full_content])
            crawled_urls.add(article_url)
            print(f"Đã crawl bài {article_url} - Thời gian: {time_text}")
            return True
            
        except TimeoutException:
            print(f"Timeout khi tải {article_url}, thử lại {attempt+1}/3")
            time.sleep(random.uniform(2, 5))
        except (NoSuchElementException, StaleElementReferenceException) as e:
            print(f"Lỗi phần tử khi tải {article_url}: {e}")
            return False
        except Exception as e:
            print(f"Lỗi không xác định khi tải {article_url}: {e}")
            return False
    return False

# 🏁 Bắt đầu quá trình crawl
driver = init_driver()
crawled_urls = load_crawled_urls(csv_file)
article_count = 0
max_articles_before_restart = 100

try:
    driver.get(base_url)
    wait_for_element(driver, By.CSS_SELECTOR, 'ul.parent > li', timeout=10)
    
    soup_categories_paper = BeautifulSoup(driver.page_source, 'html.parser')
    soup_categories = soup_categories_paper.select('ul.parent > li')

    # Mở file ở chế độ append để không mất dữ liệu cũ
    file_mode = 'a' if crawled_urls else 'w'
    write_header = not crawled_urls
    
    with open(csv_file, mode=file_mode, encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(["Source", "URL", "Category", "Keyword", "Time", "Title", "Content"])
        
        if soup_categories:
            for li in soup_categories:
                ul_tags = li.select('ul.sub')
                for ul_tag in ul_tags:
                    sub_lis = ul_tag.find_all('li')
                    for sub_li in sub_lis:
                        a_tag = sub_li.select_one('a')
                        if not a_tag:
                            continue
                            
                        href_a_sub_li = a_tag.get("href", "")
                        if not href_a_sub_li:
                            continue
                            
                        name_category = a_tag.get_text(strip=True)
                        if not href_a_sub_li.startswith('http'):
                            href_a_sub_li = base_url + href_a_sub_li

                        try:
                            print(f"Đang truy cập danh mục: {name_category} ({href_a_sub_li})")
                            driver.get(href_a_sub_li)
                            wait_for_element(driver, By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news', timeout=10)
                            
                            soup_paper = BeautifulSoup(driver.page_source, 'html.parser')

                            # Tìm phân trang
                            pagination_links = soup_paper.select('div.button-page a')
                            page_numbers = [int(link.text) for link in pagination_links if link.text.isdigit()]
                            last_page = max(page_numbers) if page_numbers else 1

                            print(f"Tìm thấy {last_page} trang cho danh mục: {name_category}")

                            for page in range(1, last_page + 1):
                                stop_category = False
                                try:
                                    page_url = f'{href_a_sub_li}-p{page}' if page > 1 else href_a_sub_li
                                    print(f"Đang xử lý trang {page}/{last_page}: {page_url}")
                                    
                                    driver.get(page_url)
                                    wait_for_element(driver, By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news', timeout=10)
                                    
                                    soup_data_paper = BeautifulSoup(driver.page_source, 'html.parser')
                                    data_paper = soup_data_paper.select('div.list-news-subfolder > article.item-news, article.item-news')

                                    if not data_paper:
                                        print(f"Không tìm thấy bài viết nào trong trang {page}")
                                        continue

                                    # Thu thập danh sách URL từ trang hiện tại
                                    article_urls = []
                                    for data in data_paper:
                                        href_article = data.select_one('h2.title-news > a, h3.title-news > a, a.title-news')
                                        if href_article:
                                            href_article_data = href_article.get("href", "")
                                            if not href_article_data:
                                                continue
                                                
                                            if not href_article_data.startswith('http'):
                                                href_article_data = base_url + href_article_data

                                            if href_article_data not in crawled_urls:
                                                article_urls.append(href_article_data)

                                    # Crawl các bài viết đã thu thập
                                    for article_url in article_urls:
                                        # Kiểm tra và khởi động lại driver nếu cần
                                        if article_count >= max_articles_before_restart:
                                            print("Khởi động lại driver để làm mới tài nguyên.")
                                            driver.quit()
                                            driver = init_driver()
                                            article_count = 0
                                        
                                        if not crawl_article(driver, article_url, name_category, writer, crawled_urls):
                                            print("Khởi động lại driver do lỗi nghiêm trọng.")
                                            driver.quit()
                                            driver = init_driver()
                                            continue
                                        
                                        article_count += 1
                                        
                                        # Nghỉ ngẫu nhiên để tránh bị chặn
                                        sleep_time = random.uniform(1, 3)
                                        time.sleep(sleep_time)

                                    if stop_category:
                                        print(f"Dừng tại trang {page} của danh mục {name_category}")
                                        break
                                    
                                    # Nghỉ giữa các trang
                                    sleep_time = random.uniform(2, 4)
                                    print(f"Nghỉ {sleep_time:.2f} giây trước khi tiếp tục...")
                                    time.sleep(sleep_time)

                                except Exception as e:
                                    print(f"Lỗi khi tải trang {page_url}: {e}")
                                    continue

                        except Exception as e:
                            print(f"Lỗi khi lấy danh mục {href_a_sub_li}: {e}")
                            continue

        else:
            print('Không tìm thấy menu')

except Exception as e:
    print(f"Lỗi chính: {e}")

finally:
    driver.quit()

print("Hoàn tất quá trình thu thập dữ liệu.")