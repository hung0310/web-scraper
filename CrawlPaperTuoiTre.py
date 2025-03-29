from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time
import csv
import random
from datetime import datetime

yesterday = datetime.now() - timedelta(days=1)

# Hàm khởi tạo driver
def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    return driver

# Hàm chờ phần tử
def wait_for_element(driver, by, value, timeout=120):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

# Hàm crawl bài báo
def crawl_article(driver, article_href, writer, crawled_urls):
    if article_href in crawled_urls:
        print(f"Bài {article_href} đã được crawl, bỏ qua.")
        return True
    
    for attempt in range(3):
        try:
            driver.get(article_href)
            wait_for_element(driver, By.CLASS_NAME, "detail-content", timeout=120)
            
            soup_article = BeautifulSoup(driver.page_source, 'html.parser')
            
            time_elem = soup_article.select_one('div.detail-time > div')
            time_paper = time_elem.get_text(strip=True) if time_elem else "N/A"
            
            title_elem = soup_article.select_one('h1.detail-title')
            if title_elem:
                title_paper = ''.join([t for t in title_elem.contents if isinstance(t, str)]).strip()
            else:
                title_paper = "Không có tiêu đề"
            
            content_elems = soup_article.select('div.detail-content p')
            content_paper = " ".join([p.get_text(strip=True) for p in content_elems if p])
            
            writer.writerow([time_paper, title_paper, content_paper])
            crawled_urls.add(article_href)
            return True
        except TimeoutException as e:
            print(f"Timeout khi tải {article_href}, thử lại {attempt+1}/3: {e}")
            time.sleep(random.uniform(2, 5))
            if attempt == 2:
                print(f"Bỏ qua bài {article_href} sau 3 lần thử")
                return False
        except Exception as e:
            print(f"Lỗi khác khi tải {article_href}: {e}")
            return False

# Hàm đọc các URL đã crawl từ file CSV
def load_crawled_urls(csv_file):
    crawled_urls = set()
    try:
        with open(csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # Bỏ qua header
            for row in reader:
                if len(row) >= 2:  # Đảm bảo có ít nhất thời gian và tiêu đề
                    # Giả sử URL không được lưu trực tiếp, dùng tiêu đề làm khóa duy nhất
                    crawled_urls.add(row[1])  # Dùng tiêu đề để tránh lặp
    except FileNotFoundError:
        pass  # Nếu file chưa tồn tại, trả về set rỗng
    return crawled_urls

# Khởi tạo driver ban đầu
driver = init_driver()
base_url = 'https://tuoitre.vn'
csv_file = 'dataset_paper_tuoitre.csv'

# Tải các URL đã crawl từ file CSV (dùng tiêu đề để kiểm tra)
crawled_titles = load_crawled_urls(csv_file)

try:
    driver.get(base_url)
    soup_categories_paper = BeautifulSoup(driver.page_source, 'html.parser')
    soup_categories = soup_categories_paper.select('ul.menu-nav > li')
    
    with open(csv_file, mode='a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(["Thời gian", "Tiêu đề", "Nội dung"])
        
        if not soup_categories:
            print("Không tìm thấy danh mục!")
            raise Exception("No categories found")
        
        href_categories = [cat.select_one('a')['href'] for cat in soup_categories]
        
        for href_category in href_categories:
            driver.get(f'{base_url}{href_category}')
            time.sleep(2)
            
            soup_category = BeautifulSoup(driver.page_source, 'html.parser')
            category_child = soup_category.select('ul.sub-category > li')
            
            for child in category_child:
                href_category_child = child.select_one('a')['href']
                category_name = href_category_child.split("/")[-1].replace(".htm", "")
                
                if category_name == 'but-bi':
                    continue
                
                driver.get(f'{base_url}{href_category_child}')
                time.sleep(2)
                
                article_hrefs = set()
                last_height = driver.execute_script("return document.body.scrollHeight")
                stop_scrolling = False
                
                while not stop_scrolling:
                    soup_articles = BeautifulSoup(driver.page_source, 'html.parser')
                    articles = soup_articles.select('div.box-category-middle > div.box-category-item')
                    
                    for article in articles:
                        article_href = article.select_one('a')['href']
                        if article_href:
                            full_href = f'{base_url}{article_href}'
                            date_str = article_href.split("-")[-1][:8]
                            try:
                                article_date = datetime.strptime(date_str, "%Y%m%d")
                                if article_date.date() == yesterday.date() and full_href not in article_hrefs:
                                    article_hrefs.add(full_href)
                                elif article_date.date() > yesterday.date() or article_date.date() < yesterday.date():
                                    print(f"Phát hiện bài từ năm 2024 trong {category_name}, dừng scroll.")
                                    stop_scrolling = True
                                    break
                            except ValueError:
                                continue
                    
                    if stop_scrolling:
                        break
                    
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3)
                    
                    try:
                        btn = wait_for_element(driver, By.CLASS_NAME, "view-more")
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                    except:
                        pass
                    
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        print(f"Đã load hết bài báo trong {category_name}: {len(article_hrefs)} bài")
                        break
                    last_height = new_height
                
                print(f"Bắt đầu crawl {len(article_hrefs)} bài trong {category_name}")
                article_list = list(article_hrefs)  # Chuyển thành danh sách để kiểm soát
                for article_href in article_list:
                    # Kiểm tra xem bài đã được crawl chưa dựa trên URL
                    success = crawl_article(driver, article_href, writer, crawled_titles)
                    if not success:
                        print("Khởi động lại driver do lỗi nghiêm trọng.")
                        driver.quit()
                        driver = init_driver()
                        # Sau khi reset, không cần crawl lại từ đầu, tiếp tục với các bài chưa crawl
                        continue

except Exception as e:
    print(f"Lỗi chính: {e}")
finally:
    driver.quit()