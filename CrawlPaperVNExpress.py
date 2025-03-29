import csv
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Set

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

class NewsScraper:
    def __init__(self):
        self.base_url = 'https://vnexpress.net'
        self.yesterday = (datetime.now() - timedelta(days=1)).date()
        self.output_file = 'dataset_paper_vnexpress.csv'
        self.driver = self._initialize_driver()
        self.crawled_urls = self._load_crawled_urls()

    def _initialize_driver(self) -> webdriver.Chrome:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--allow-insecure-localhost")
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(120)  # Tăng timeout lên 120 giây
        return driver

    def _wait_for_element(self, by: By, value: str, timeout: int = 120) -> WebDriverWait:
        return WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )

    def _get_soup(self, url: str, selector: str, retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(retries):
            try:
                self.driver.get(url)
                self._wait_for_element(By.CSS_SELECTOR, selector)
                return BeautifulSoup(self.driver.page_source, 'html.parser')
            except TimeoutException as e:
                print(f"Timeout khi tải {url} (lần {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 5))
            except Exception as e:
                print(f"Lỗi khác khi tải {url} (lần {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 5))
        return None

    def _extract_date_from_url(self, url: str) -> Optional[datetime.date]:
        first_url = url.split(',')[0].strip()  # Lấy URL đầu tiên trong srcset
        match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', first_url)
        return datetime(*map(int, match.groups())).date() if match else None

    def _load_crawled_urls(self) -> Set[str]:
        crawled_urls = set()
        try:
            with open(self.output_file, mode='r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader, None)  # Bỏ qua header
                for row in reader:
                    if len(row) >= 2:  # Dùng tiêu đề làm khóa để tránh trùng
                        crawled_urls.add(row[1])  # Tiêu đề ở cột thứ 2
        except FileNotFoundError:
            pass
        return crawled_urls

    def _crawl_article(self, url: str, writer: csv.writer) -> bool:
        if url in self.crawled_urls:
            print(f"Bài {url} đã được crawl, bỏ qua.")
            return True

        for attempt in range(3):
            try:
                soup = self._get_soup(url, 'div.sidebar-1 > h1.title-detail')
                if not soup:
                    raise TimeoutException("Không tải được nội dung bài viết")

                time_elem = soup.select_one('div.sidebar-1 > div.header-content > span.date')
                title_elem = soup.select_one('div.sidebar-1 > h1.title-detail')
                head_elem = soup.select_one('div.sidebar-1 > p.description')
                main_elems = soup.select('div.sidebar-1 > article.fck_detail > p.Normal')

                time_text = time_elem.get_text(strip=True) if time_elem else 'N/A'
                title_text = title_elem.get_text(strip=True) if title_elem else 'N/A'
                head_text = head_elem.get_text(strip=True) if head_elem else ''
                main_text = ' '.join(p.get_text(strip=True) for p in main_elems) if main_elems else ''

                full_content = f"{head_text} {main_text}".strip()
                writer.writerow([time_text, title_text, full_content])
                self.crawled_urls.add(title_text)  # Dùng tiêu đề để kiểm tra trùng
                return True

            except TimeoutException as e:
                print(f"Timeout khi tải {url} (lần {attempt + 1}/3): {e}")
                if attempt == 2:
                    print(f"Bỏ qua bài {url} sau 3 lần thử")
                    return False
                time.sleep(random.uniform(2, 5))
            except Exception as e:
                print(f"Lỗi khác khi tải {url}: {e}")
                return False
        return False

    def scrape_category(self, category_url: str, category_name: str, writer: csv.writer):
        soup = self._get_soup(category_url, 'div.list-news-subfolder > article.item-news')
        if not soup:
            return

        pagination_links = soup.select('div.button-page a')
        last_page = max((int(link.text) for link in pagination_links if link.text.isdigit()), default=1)
        print(f"Đang lấy dữ liệu từ {last_page} trang cho danh mục: {category_name}")

        article_urls = set()
        for page in range(1, last_page + 1):
            page_url = f'{category_url}-p{page}'
            soup = self._get_soup(page_url, 'div.list-news-subfolder > article.item-news')
            if not soup:
                continue

            articles = soup.select('div.list-news-subfolder > article.item-news')
            stop_category = False

            for article in articles:
                srcset_elem = article.select_one('div.thumb-art > a > picture > source')
                if not srcset_elem or not srcset_elem.get('srcset'):
                    continue

                article_date = self._extract_date_from_url(srcset_elem['srcset'])
                if not article_date or article_date != self.yesterday:
                    stop_category = True
                    break

                href_elem = article.select_one('h2.title-news > a')
                if href_elem and href_elem.get('href'):
                    article_url = href_elem['href']
                    if not article_url.startswith('http'):
                        article_url = self.base_url + article_url
                    article_urls.add(article_url)

            if stop_category:
                break
            time.sleep(random.uniform(2, 4))

        print(f"Bắt đầu crawl {len(article_urls)} bài trong {category_name}")
        for article_url in article_urls:
            success = self._crawl_article(article_url, writer)
            if not success:
                print("Khởi động lại driver do lỗi nghiêm trọng.")
                self.driver.quit()
                self.driver = self._initialize_driver()
                continue

    def run(self):
        try:
            soup = self._get_soup(self.base_url, 'ul.parent > li')
            if not soup:
                print('Không tìm thấy menu')
                return

            with open(self.output_file, mode='a', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                if file.tell() == 0:  # Chỉ ghi header nếu file rỗng
                    writer.writerow(["Thời gian", "Tiêu đề", "Nội dung"])

                for category in soup.select('ul.parent > li'):
                    for sub_category in category.select('ul.sub > li'):
                        name = sub_category.get_text(strip=True)
                        a_tag = sub_category.select_one('a')
                        if a_tag and a_tag.get('href'):
                            url = a_tag['href']
                            if not url.startswith('http'):
                                url = self.base_url + url
                            self.scrape_category(url, name, writer)

        except Exception as e:
            print(f"Lỗi chính: {e}")
        finally:
            self.driver.quit()

if __name__ == '__main__':
    scraper = NewsScraper()
    scraper.run()