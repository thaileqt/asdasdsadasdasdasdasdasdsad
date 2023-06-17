import os
import glob
import requests
import json
import csv

def get_current_cat_id(save_folder, min=1):
    csv_files = glob.glob(save_folder + "/" + "*.csv")
    if len(csv_files) != 0:
        return max(map(lambda x: int(x.split('/')[1].split('.')[0]), csv_files))
    else:
        return min

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


if __name__ == '__main__':
    save_folder = 'data4'
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    # Check for failed_cat_id every n steps
    step = 500
    for i in range(4):
        # Get current cat_id in save_folder
        # current = get_current_cat_id(save_folder, min=22000)
        current = 30000
        crawl(range(current+i*step, current+step+i*step), save_folder, logs=True)
