import re
import json
import requests
from bs4 import BeautifulSoup
import logging
import threading


logger = logging.getLogger('1mgscrape')
handler = logging.FileHandler('product.logs')
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


content = requests.get('https://www.1mg.com/').text
bs4_instance = BeautifulSoup(content)
menus = bs4_instance.find_all('li', {'class': 'ChildrenItem__item___2j7VT'})


def get_all_data():
    all_data = []
    for c, menu in enumerate(menus):
        submenu = menu.findChildren()
        for link in submenu:
            text = str(link)
            data = {}
            pattern = re.compile(
                r'ChildItem__level-2___9pdT2"\s+.+\s+href="(.+)"><span.+>(.+)</span>')
            link_and_text = pattern.findall(text)
            if link_and_text:
                data['link'] = 'https://www.1mg.com' + link_and_text[0][0]
                data['text'] = link_and_text[0][1]
                data['id'] = c
                all_data.append(data)
    return all_data


def scrape_product(data):
    req_data = {}
    link = data.get('link')
    medicine_list_page = requests.get(link).text
    bs4_instance_medicine_list = BeautifulSoup(medicine_list_page)
    pattern = re.compile(r'href="(.*)"\s+target=')
    medicine_list = bs4_instance_medicine_list.find_all(
        'a', {'class': 'style__product-link___UB_67'})
    for medicine in medicine_list:
        try:
            link = 'https://www.1mg.com' + pattern.findall(str(medicine))[0]
            detail_response_medicine = requests.get(link).text
            product_pattern = re.compile(r'window.__INITIAL_STATE__ = (.+);\n')
            product_data = product_pattern.findall(detail_response_medicine)[0]
            product = json.loads(product_data)
            schema = product.get('otcReducer').get('data').get('schema')
            req_data['category_name'] = data.get('text')
            req_data['category_id'] = data.get('id')
            req_data['name'] = schema.get('product', {}).get('sku')
            req_data['brand'] = schema.get('product', {}).get('brand')
            req_data['description'] = BeautifulSoup(
                schema.get('product', {}).get('description')).get_text()
            logger.info(json.dumps(req_data))
        except:
            pass


def scrape():
    thread_list, all_data = [], get_all_data()
    try:
        file_parsed = open('product.logs', 'r').readlines()
        last_parsed = json.loads(file_parsed[-1])
        last_parsed_id = int(last_parsed.get('category_id'))
        all_data = filter(lambda x: x.get('id') >= last_parsed_id, all_data)
    except (FileNotFoundError, IndexError):
        pass

    for data in all_data:
        if len(thread_list) <= 10:
            t1 = threading.Thread(target=scrape_product, args=(data,))
            t1.start()
            thread_list.append(t1)
        else:
            for thread in thread_list:
                thread.join()
            t1 = threading.Thread(target=scrape_product, args=(data,))
            t1.start()
            thread_list = [t1]


if __name__ == '__main__':
    scrape()
