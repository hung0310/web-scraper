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
options.add_argument("--headless")  # Bỏ comment dòng này nếu muốn chạy ẩn

driver = webdriver.Chrome(options=options)
driver.set_page_load_timeout(180)

base_url = 'https://vnexpress.net'
csv_file = 'dataset_paper_vnexpress.csv'
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
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.parent > li')))
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
                                WebDriverWait(driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news'))
                                )
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
                                        WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.list-news-subfolder > article.item-news, article.item-news'))
                                        )
                                        soup_data_paper = BeautifulSoup(driver.page_source, 'html.parser')
                                        data_paper = soup_data_paper.select('div.list-news-subfolder > article.item-news, article.item-news')

                                        if not data_paper:
                                            print(f"Không tìm thấy bài viết nào trong trang {page}")
                                            continue

                                        for data in data_paper:
                                            srcset_elem = data.select_one('div.thumb-art > a > picture > source, source')
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
                                            
                                            href_article = data.select_one('h2.title-news > a, h3.title-news > a, a.title-news')
                                            if href_article:
                                                href_article_data = href_article.get("href", "")
                                                if not href_article_data:
                                                    continue
                                                    
                                                if not href_article_data.startswith('http'):
                                                    href_article_data = base_url + href_article_data

                                                if href_article_data in processed_urls:
                                                    print(f"Đã xử lý bài viết này rồi: {href_article_data}")
                                                    continue

                                                try:
                                                    print(f"Đang xử lý bài viết: {href_article_data}")
                                                    driver.get(href_article_data)
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
                                                        print(f"Không tìm thấy nội dung cho bài viết: {href_article_data}")
                                                        continue

                                                    writer.writerow(["VN Express", href_article_data, name_category, keyword_paper, time_text, title_text, full_content])
                                                    processed_urls.add(href_article_data)
                                                    print(f"Đã lưu bài viết: {title_text}")

                                                except Exception as e:
                                                    print(f"Lỗi khi lấy bài viết {href_article_data}: {e}")
                                                    continue

                                        if stop_category:
                                            print(f"Dừng tại trang {page} của danh mục {name_category} vì không còn bài từ hôm qua.")
                                            break
                                        
                                        # Nghỉ ngẫu nhiên để tránh bị chặn
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
                            print(f"Không tìm thấy liên kết trong mục {text_sub}")

        else:
            print('Không tìm thấy menu')

except Exception as e:
    print(f"Lỗi khi mở trang chủ: {e}")

finally:
    driver.quit()

print("Hoàn tất quá trình thu thập dữ liệu.")