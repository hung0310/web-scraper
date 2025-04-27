import os
import time
import csv
import random
import logging
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
yesterday = datetime.now(vn_timezone) - timedelta(days=1)
csv_file = 'dataset_paper_tuoitre.csv'
base_url = 'https://tuoitre.vn'

# Cấu hình logging
logging.basicConfig(filename='crawler.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

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
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(120)
        logging.info("Khởi tạo driver thành công.")
        return driver
    except Exception as e:
        logging.error(f"Lỗi khởi tạo driver: {e}")
        raise

# 🛠 Hàm chờ phần tử có thể nhấp vào
def wait_for_element(driver, by, value, timeout=10):
    try:
        return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
    except TimeoutException:
        return None

# 🛠 Hàm chờ trang tải
def wait_for_page_load(driver, timeout=10):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        logging.warning("Timeout khi chờ trang tải.")

# 🛠 Hàm click nút "Xem thêm"
def click_view_more(driver, timeout=5):
    try:
        btn = wait_for_element(driver, By.CLASS_NAME, "view-more", timeout=timeout)
        if btn:
            driver.execute_script("arguments[0].click();", btn)
            wait_for_page_load(driver)
            logging.info("Click nút 'Xem thêm' thành công.")
            return True
        logging.info("Không tìm thấy nút 'Xem thêm'.")
        return False
    except TimeoutException:
        logging.info("Timeout khi chờ nút 'Xem thêm'.")
        return False
    except Exception as e:
        logging.error(f"Lỗi khi click 'Xem thêm': {e}")
        return False

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

# 🛠 Hàm crawl bài báo
def crawl_article(driver, category_name, article_href, writer, crawled_urls):
    if article_href in crawled_urls:
        logging.info(f"Bài {article_href} đã được crawl, bỏ qua.")
        return True
    for attempt in range(3):
        try:
            driver.get(article_href)
            wait_for_element(driver, By.CLASS_NAME, "detail-content", timeout=15)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            time_elem = soup.select_one('div.detail-time > div')
            time_paper = time_elem.get_text(strip=True) if time_elem else "N/A"
            title_elem = soup.select_one('h1.detail-title')
            title_paper = title_elem.get_text(strip=True) if title_elem else "Không có tiêu đề"
            content_elems = soup.select('div.detail-content p')
            content_paper = " ".join([p.get_text(strip=True) for p in content_elems if p])
            keyword_elems = soup.select('div.detail-tab > a')
            keyword_paper = ",".join([a.get_text(strip=True) for a in keyword_elems if a])
            writer.writerow(["Tuoi tre", article_href, category_name, keyword_paper, time_paper, title_paper, content_paper])
            crawled_urls.add(article_href)
            logging.info(f"Crawl thành công {article_href}")
            return True
        except TimeoutException:
            logging.warning(f"Timeout khi tải {article_href}, thử lại {attempt+1}/3")
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            logging.error(f"Lỗi khi tải {article_href}: {e}")
            with open('failed_urls.txt', 'a', encoding='utf-8') as f:
                f.write(f"{article_href}\n")
            return False
    logging.error(f"Thất bại sau 3 lần thử: {article_href}")
    return False

# 🏁 Bắt đầu quá trình crawl
driver = init_driver()
crawled_urls = load_crawled_urls(csv_file)
MAX_ARTICLES_PER_CATEGORY = 50

try:
    driver.get(base_url)
    wait_for_page_load(driver)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    categories = soup.select('ul.menu-nav > li')

    with open(csv_file, mode='w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Source", "URL", "Category", "Keyword", "Time", "Title", "Content"])

        for cat in categories:
            cat_link = cat.select_one('a')
            if not cat_link:
                logging.warning("Không tìm thấy link danh mục chính.")
                continue
            href_category = cat_link['href']
            category_url = f"{base_url}{href_category}"
            category_name = cat_link.get_text(strip=True)
            logging.info(f"Bắt đầu crawl danh mục chính: {category_name} ({category_url})")
            driver.get(category_url)
            wait_for_page_load(driver)

            # Crawl danh mục con
            soup_category = BeautifulSoup(driver.page_source, 'html.parser')
            sub_categories = soup_category.select('ul.sub-category > li')

            for sub_cat in sub_categories:
                sub_cat_link = sub_cat.select_one('a')
                if not sub_cat_link:
                    logging.warning("Không tìm thấy link danh mục con.")
                    continue
                href_sub_category = sub_cat_link['href']
                sub_category_url = f"{base_url}{href_sub_category}"
                sub_category_name = sub_cat_link.get_text(strip=True)

                if sub_category_name.lower() == 'bút bi':
                    logging.info("Bỏ qua danh mục con 'but-bi'.")
                    continue

                logging.info(f"Bắt đầu crawl danh mục con: {sub_category_name} ({sub_category_url})")
                driver.get(sub_category_url)
                wait_for_page_load(driver)

                article_hrefs = set()
                last_height = driver.execute_script("return document.body.scrollHeight")
                stop_scroll = False

                while not stop_scroll:
                    soup_articles = BeautifulSoup(driver.page_source, 'html.parser')
                    articles = soup_articles.select('div.box-category-item > a')
                    
                    for article in articles:
                        article_href = article['href']
                        if not article_href.startswith('http'):
                            article_href = f"{base_url}{article_href}"
                        date_str = article['href'].split("-")[-1][:8]
                        try:
                            article_date = datetime.strptime(date_str, "%Y%m%d").date()
                            if article_date.year >= 2025:
                                article_hrefs.add(article_href)
                            else:
                                logging.info(f"Dừng scroll trong {sub_category_name}, phát hiện bài cũ.")
                                stop_scroll = True
                                break
                        except ValueError:
                            continue

                    if click_view_more(driver):
                        continue

                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    wait_for_page_load(driver)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        logging.info(f"Đã load hết bài báo trong {sub_category_name}: {len(article_hrefs)} bài")
                        break
                    last_height = new_height

                logging.info(f"Tìm thấy {len(article_hrefs)} bài trong {sub_category_name}")

                for article_href in list(article_hrefs)[:MAX_ARTICLES_PER_CATEGORY]:
                    if not crawl_article(driver, sub_category_name, article_href, writer, crawled_urls):
                        logging.error("Khởi động lại driver do lỗi nghiêm trọng.")
                        try:
                            driver.quit()
                            driver = init_driver()
                        except Exception as e:
                            logging.error(f"Lỗi khi khởi động lại driver: {e}")
                            raise
                        continue

except Exception as e:
    logging.error(f"Lỗi chính: {e}")
finally:
    try:
        driver.quit()
        logging.info("Đóng driver thành công.")
    except Exception as e:
        logging.error(f"Lỗi khi đóng driver: {e}")