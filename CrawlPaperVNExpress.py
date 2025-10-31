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
import logging
import pytz

# Cấu hình logging
logging.basicConfig(level=logging.INFO, filename='vnexpress_crawler.log', 
                   format='%(asctime)s - %(levelname)s - %(message)s')

base_url = 'https://vnexpress.net'
csv_file = 'dataset_paper_vnexpress.csv'

vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
yesterday = datetime.now(vn_timezone) - timedelta(days=1)

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

def _extract_date_from_url(srcset):
    try:
        url = srcset.split(',')[0].strip().split(' ')[0]
        parts = url.split('/')
        for i in range(len(parts) - 2):
            if (i+2 < len(parts) and
                parts[i].isdigit() and len(parts[i]) == 4 and
                parts[i+1].isdigit() and len(parts[i+1]) == 2 and
                parts[i+2].isdigit() and len(parts[i+2]) == 2):
                date_str = f"{parts[i]}/{parts[i+1]}/{parts[i+2]}"
                return datetime.strptime(date_str, '%Y/%m/%d').date()
        logging.warning(f"Không tìm thấy định dạng ngày trong: {srcset}")
        return None
    except Exception as e:
        logging.error(f"Lỗi khi trích xuất ngày từ srcset: {e}")
        return None

# 🛠 Hàm crawl bài báo
def crawl_article(driver, article_url, category_name, writer, crawled_urls):
    if article_url in crawled_urls:
        logging.info(f"Bài {article_url} đã được crawl, bỏ qua.")
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
                logging.warning(f"Không tìm thấy nội dung cho bài viết: {article_url}")
                return False

            writer.writerow(["VN Express", article_url, category_name, keyword_paper, time_text, title_text, full_content])
            crawled_urls.add(article_url)
            logging.info(f"Đã crawl bài {article_url}")
            return True
            
        except TimeoutException:
            logging.warning(f"Timeout khi tải {article_url}, thử lại {attempt+1}/3")
            time.sleep(random.uniform(2, 5))
        except (NoSuchElementException, StaleElementReferenceException) as e:
            logging.error(f"Lỗi phần tử khi tải {article_url}: {e}")
            return False
        except Exception as e:
            logging.error(f"Lỗi không xác định khi tải {article_url}: {e}")
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

    with open(csv_file, mode='w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
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
                            logging.info(f"Đang truy cập danh mục: {name_category} ({href_a_sub_li})")
                            driver.get(href_a_sub_li)
                            wait_for_element(driver, By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news', timeout=10)
                            
                            soup_paper = BeautifulSoup(driver.page_source, 'html.parser')

                            # Tìm phân trang
                            pagination_links = soup_paper.select('div.button-page a')
                            page_numbers = [int(link.text) for link in pagination_links if link.text.isdigit()]
                            last_page = max(page_numbers) if page_numbers else 1

                            logging.info(f"Tìm thấy {last_page} trang cho danh mục: {name_category}")

                            for page in range(1, last_page + 1):
                                stop_category = False
                                try:
                                    page_url = f'{href_a_sub_li}-p{page}' if page > 1 else href_a_sub_li
                                    logging.info(f"Đang xử lý trang {page}/{last_page}: {page_url}")
                                    
                                    driver.get(page_url)
                                    wait_for_element(driver, By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news', timeout=10)
                                    
                                    soup_data_paper = BeautifulSoup(driver.page_source, 'html.parser')
                                    data_paper = soup_data_paper.select('div.list-news-subfolder > article.item-news, article.item-news')

                                    if not data_paper:
                                        logging.warning(f"Không tìm thấy bài viết nào trong trang {page}")
                                        continue

                                    # Thu thập danh sách URL từ trang hiện tại
                                    article_urls = []
                                    for data in data_paper:
                                        srcset_elem = data.select_one('div.thumb-art > a > picture > source, source')
                                        if not srcset_elem or not srcset_elem.get('srcset'):
                                            continue

                                        article_date = _extract_date_from_url(srcset_elem['srcset'])
                                        if not article_date:
                                            continue
                                            
                                        logging.info(f"Ngày bài viết: {article_date}, Ngày hôm qua: {yesterday.date()}")
                                        
                                        if article_date < yesterday.date():
                                            logging.info(f"Bài viết cũ hơn ngày hôm qua, dừng danh mục này.")
                                            stop_category = True
                                            break
                                        elif article_date > yesterday.date():
                                            logging.info(f"Bài viết mới hơn ngày hôm qua, bỏ qua.")
                                            continue
                                        
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
                                            logging.info("Khởi động lại driver để làm mới tài nguyên.")
                                            driver.quit()
                                            driver = init_driver()
                                            article_count = 0
                                        
                                        if not crawl_article(driver, article_url, name_category, writer, crawled_urls):
                                            logging.warning("Khởi động lại driver do lỗi nghiêm trọng.")
                                            driver.quit()
                                            driver = init_driver()
                                            continue
                                        
                                        article_count += 1
                                        
                                        # Nghỉ ngẫu nhiên để tránh bị chặn
                                        sleep_time = random.uniform(1, 3)
                                        time.sleep(sleep_time)

                                    if stop_category:
                                        logging.info(f"Dừng tại trang {page} của danh mục {name_category}")
                                        break
                                    
                                    # Nghỉ giữa các trang
                                    sleep_time = random.uniform(2, 4)
                                    logging.info(f"Nghỉ {sleep_time:.2f} giây trước khi tiếp tục...")
                                    time.sleep(sleep_time)

                                except Exception as e:
                                    logging.error(f"Lỗi khi tải trang {page_url}: {e}")
                                    continue

                        except Exception as e:
                            logging.error(f"Lỗi khi lấy danh mục {href_a_sub_li}: {e}")
                            continue

        else:
            logging.warning('Không tìm thấy menu')

except Exception as e:
    logging.error(f"Lỗi chính: {e}")

finally:
    driver.quit()

logging.info("Hoàn tất quá trình thu thập dữ liệu.")
