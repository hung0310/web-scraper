from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv
import random
import os

options = Options()
options.add_argument("--headless")

driver = webdriver.Chrome(options=options)
driver.set_page_load_timeout(120)

base_url = 'https://vietnamnet.vn'
csv_file = 'dataset_paper_vnnet.csv'
yesterday = datetime.now() - timedelta(days=1)
processed_urls = set()

def _extract_date_from_url(srcset):
    try:
        # Trích xuất URL đầu tiên từ srcset
        url = srcset.split(',')[0].strip().split(' ')[0]
        parts = url.split('/')
        for i in range(len(parts) - 2):
            if (i+2 < len(parts) and
                parts[i].isdigit() and len(parts[i]) == 4 and
                parts[i+1].isdigit() and len(parts[i+1]) == 2 and
                parts[i+2].isdigit() and len(parts[i+2]) == 2):
                date_str = f"{parts[i]}/{parts[i+1]}/{parts[i+2]}"
                return datetime.strptime(date_str, '%Y/%m/%d').date()
        print(f"Không tìm thấy định dạng ngày trong: {srcset}")
        return None
    except Exception as e:
        print(f"Lỗi khi trích xuất ngày từ srcset: {e}")
        return None

try:
    driver.get(base_url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.mainNav__list > li')))
    soup_categories_paper = BeautifulSoup(driver.page_source, 'html.parser')
    soup_categories = soup_categories_paper.select('ul.mainNav__list > li')
    
    with open(csv_file, mode='w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Source", "URL", "Category", "Keyword", "Time", "Title", "Content"])
        
        if soup_categories:
            for li in soup_categories[1:]:
                ul_tags = li.select('ul.sub__menu')
                for ul_tag in ul_tags:
                    sub_lis = ul_tag.find_all('li')
                    for sub_li in sub_lis:
                        text_sub = sub_li.get_text(strip=True)
                        a_tag = sub_li.select_one('a')
                        if a_tag:
                            href_a_sub_li = a_tag.get("href", "")
                            if not href_a_sub_li:
                                continue
                            
                            name_category = a_tag.get_text(strip=True)
                            if not href_a_sub_li.startswith('http'):
                                href_a_sub_li = base_url + href_a_sub_li
                                
                            try:
                                print(f"Đang truy cập danh mục: {name_category} ({href_a_sub_li})")
                                driver.get(href_a_sub_li)
                                soup_paper = BeautifulSoup(driver.page_source, 'html.parser')
                                
                                # Tìm phân trang
                                pagination_links = soup_paper.select('div.pagination > ul.pagination__list > li.pagination__list-item > a')
                                page_numbers = [int(link.text) for link in pagination_links if link.text.isdigit()]
                                last_page = max(page_numbers) if page_numbers else 1
                                
                                for page in range(1, last_page + 1):
                                    stop_category = False
                                    try:
                                        page_url = f'{href_a_sub_li}-page{page}' if page > 1 else href_a_sub_li
                                        print(f"Đang xử lý trang {page}/{last_page}: {page_url}")
                                        driver.get(page_url)
                                        WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.topStory-15nd > div.horizontalPost, div.horizontalPost'))
                                        )
                                        soup_data_paper = BeautifulSoup(driver.page_source, 'html.parser')
                                        data_paper = soup_data_paper.select('div.topStory-15nd > div.horizontalPost, div.horizontalPost')
                                        
                                        if not data_paper:
                                            print(f"Không tìm thấy bài viết nào trong trang {page}")
                                            continue
                                        
                                        for data in data_paper:
                                            srcset_elem = data.select_one('div.horizontalPost__avt > a > picture > source, source')
                                            if not srcset_elem or not srcset_elem.get('srcset'):
                                                continue
                                            
                                            article_date = _extract_date_from_url(srcset_elem['srcset'])
                                            if not article_date:
                                                print("Không thể xác định ngày của bài viết, tiếp tục.")
                                                continue
                                                
                                            print(f"Ngày bài viết: {article_date}, Ngày hôm qua: {yesterday.date()}")
                                            
                                            if article_date < yesterday.date():
                                                print(f"Bài viết cũ hơn ngày hôm qua, dừng danh mục này.")
                                                stop_category = True
                                                break
                                            elif article_date > yesterday.date():
                                                print(f"Bài viết mới hơn ngày hôm qua, bỏ qua.")
                                                continue
                                        
                                    except Exception as e:
                                        print(f"Lỗi khi tải trang {page_url}: {e}")
                                        continue    
                                
                            except Exception as e:
                                print(f"Lỗi khi lấy danh mục {href_a_sub_li}: {e}")
                                continue
        
except Exception as e:
    print(f"Lỗi khi mở trang chủ: {e}")

finally:
    driver.quit()

print("Hoàn tất quá trình thu thập dữ liệu.")