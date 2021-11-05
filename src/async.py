import time
from bs4 import BeautifulSoup
import requests
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from slugify import slugify
# from rich.console import Console

# console = Console()
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0'
}
start_time = time.time()

# more workers faster scrapes less time
# more likely to get timed out by server

workers = 30
JSONWorkers = 1
detailWorkers = 30


# can do en and ar
def getGardenia():
    json_file = open('datasets/Gardenia.json', 'w', encoding='utf-8')
    json_file_details = open(
        'datasets/GardeniaDetails.json', 'w', encoding='utf-8')
    pool = ThreadPoolExecutor(max_workers=workers)
    poolDetails = ThreadPoolExecutor(max_workers=detailWorkers)
    poolJSON = ThreadPoolExecutor(max_workers=JSONWorkers)

    def getMedDetails(link, name):
        # console.print(link, style="bold blue")
        document = requests.get(link, headers=headers)
        soup = BeautifulSoup(document.content, 'html.parser')
        element = soup.find(
            'div', class_='woocommerce-Tabs-panel woocommerce-Tabs-panel--description panel entry-content wc-tab')
        if element != None:
            obj = {hashlib.md5(slugify(name).encode()
                               ).hexdigest(): element.get_text()}
            # console.print(obj, style="bold red")
            poolJSON.submit(json.dump, obj, json_file_details,
                            ensure_ascii=False, indent=4)

    def getMeds(page):
        URL = f'https://gardeniapharmacy.com/product-category/find-a-medicine/page/{page}/?lang=en'
        document = requests.get(URL, headers=headers)
        soup = BeautifulSoup(document.content, 'html.parser')
        for element in soup.find_all('div', class_='astra-shop-summary-wrap'):
            a = element.a
            span = element.span.get_text()
            link = a.get('href')
            name = a.get_text()
            obj = {
                'name': name, 'link': link, 'category': span[1:]
            }
            poolJSON.submit(json.dump, obj, json_file,
                            ensure_ascii=False, indent=4)
            poolDetails.submit(getMedDetails, link, name)

    for page in range(1, 221):
        pool.submit(getMeds, page)

    pool.shutdown(wait=True)
    poolDetails.shutdown(wait=True)
    poolJSON.shutdown(wait=True)

    json_file.close()
    json_file_details.close()


def getEDS():
    json_file = open('datasets/EDS.json', 'w', encoding='utf-8')
    json_file_details = open(
        'datasets/EDSDetails.json', 'w', encoding='utf-8')
    pool = ThreadPoolExecutor(max_workers=workers)
    poolDetails = ThreadPoolExecutor(max_workers=detailWorkers)
    poolJSON = ThreadPoolExecutor(max_workers=JSONWorkers)

    def generateCategoryURL():
        urls = []
        URL = f'http://egyptiandrugstore.com/index.php?route=product/category&path=59'
        document = requests.get(URL)
        soup = BeautifulSoup(document.content, 'html.parser')
        for element in soup.find_all('li', class_='accordion'):
            link = element.a.get('href')
            urls.append(link+'&page=1')
        return urls

    def getNumPages(URL):
        document = requests.get(URL)
        soup = BeautifulSoup(document.content, 'html.parser')
        div = soup.find('div', class_='results').text
        start = div.find('(')+1
        end = div[start:].find(' ')
        return int(div[start:start+end])

    def getMedDetails(URL, name):
        document = requests.get(URL, headers=headers)
        soup = BeautifulSoup(document.content, 'html.parser')
        for element in soup.find_all('div', id='tab-description'):
            obj = {hashlib.md5(slugify(name).encode()
                               ).hexdigest(): element.get_text()}
            # console.print(obj)
            poolJSON.submit(json.dump, obj, json_file_details,
                            ensure_ascii=False, indent=4)

    def getMeds(URL, page):
        # console.print(URL, style="bold cyan")
        document = requests.get(URL[:-1]+str(page), headers=headers)
        soup = BeautifulSoup(document.content, 'html.parser')
        category = soup.find('h2', id='title-page').get_text()
        for element in soup.find_all('div', class_='name'):
            element = element.a
            link = element.get('href')
            name = element.get_text()
            obj = {'link': link, 'name': name, 'category': category[:-12]}
            # console.print(obj)
            poolDetails.submit(getMedDetails, link, name)
            poolJSON.submit(json.dump, obj, json_file,
                            ensure_ascii=False, indent=4)

    catUrl = generateCategoryURL()

    def startMeds(URL):
        numOfPages = getNumPages(URL)+1
        for page in range(1, numOfPages+1):
            pool.submit(getMeds, URL, page)

    for URL in catUrl:
        startMeds(URL)

    pool.shutdown(wait=True)
    poolDetails.shutdown(wait=True)
    poolJSON.shutdown(wait=True)

    json_file.close()
    json_file_details.close()


print("time to complete: --- %s seconds ---" % (time.time() - start_time))
