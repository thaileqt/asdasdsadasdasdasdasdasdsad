import os
import glob
import requests
import json
import csv
from tqdm import tqdm
import pandas as pd



def create_filename(folder_name):
    if len(os.listdir(folder_name)) == 0:
        return f'{folder_name}/1.csv'
    csv_fn = glob.glob(f'{folder_name}/' + '*.csv')
    filename = list(map(lambda x: int(x.split('/')[-1].split('.')[0]), csv_fn))
    return f'{folder_name}/{max(filename)+1}.csv'

def last_crawl_at(folder_name, ids):
    if len(os.listdir(folder_name)) == 0:
        return 0 
    csv_fn = glob.glob(f'{folder_name}/' + '*.csv')
    file_path = list(map(lambda x: int(x.split('/')[-1].split('.')[0]), csv_fn))
    file_path = f'{folder_name}/{max(file_path)}.csv'
    return ids.index(str(pd.read_csv(file_path).tail(1)['id'].values[0]))


def get_id_list_from_file(file_path):
    with open(file_path, "r") as f:
        id_list = f.read().splitlines()
    return id_list

def save_data(products, filename):
    fieldnames = set().union(*(d.keys() for d in products))
    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for row in products:
            row = {key: value for key, value in row.items() if key in fieldnames}
            writer.writerow(row)


def crawl(cat_id_list, save_folder='data', logs=True):
    headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0"}
    url = "https://tiki.vn/api/v2/products?limit=100&category={}&page={}"
    failed_request_at = []
    for cat_id in cat_id_list:
        product_list = []
        i = 1
        max_id = max(cat_id_list)
        min_id = min(cat_id_list)
        while (True):
            response = requests.get(url.format(cat_id, i), headers=headers)
            
            if (response.status_code != 200):
                break
            try:
                products = json.loads(response.text)["data"]
            except:
                failed_request_at.append(cat_id)
                break

            if (len(products) == 0):
                break
            product_list.extend(products)

            i += 1
        if len(product_list) > 0:
            save_data(product_list, f'{save_folder}/{cat_id}.csv')
            if logs and len(cat_id_list) > 1:
                print(f'{cat_id}/{max(cat_id_list)} -- {round((cat_id-min_id)*100/(max_id-min_id), 2)}%')
    if len(failed_request_at) > 0:
        crawl(failed_request_at, save_folder, logs)


def crawl_product_by_id(id_list, save_path, checking_fail=False):
    headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}
    product_url = "https://tiki.vn/api/v2/products/{}"

    
    products = []
    failed = []
    desc = 'Crawling products' if not checking_fail else 'Crawling failed products'
    for i in tqdm(id_list, desc=desc):
        try:
            response = requests.get(product_url.format(i), headers=headers)
            info = json.loads(response.text)
        except:
            failed.append(i)
            continue
        cols = ['images', 'url_key', 'url_path', 'short_url', 'thumbnail_url']
        for c in cols:
            info.pop(c)
        products.append(info)

    if len(failed) > 0:
        products.extend(crawl_product_by_id(failed, save_path, True))
    if not checking_fail:
        save_data(products, save_path)
    else:
        return products

if __name__ == '__main__':
      
    product_id_file = 'id_list2.txt'
    folder_name = 'newdata2'

    step = 2000
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)

    ids = get_id_list_from_file(product_id_file) # 31k
    n_file = len(ids) // step
    for _ in range(n_file):
        idx = last_crawl_at(folder_name, ids)
        crawl_product_by_id(ids[idx+1:idx+1+step], create_filename(folder_name))


