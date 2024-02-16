#%%
import requests
import json
import magento_config 
import pandas as pd 
import time
from sqlalchemy import create_engine
import dbi_config as config
import pyodbc
import csv 
from datetime import datetime
import pytz
import os 
from git import Repo
from io import StringIO
import importlib
#%%
importlib.reload(magento_config)
# %%
def get_url_file(url):
    get_url = requests.get(url)
    if get_url.status_code == 200:
        content = get_url.text
        url_file = pd.read_csv(StringIO(content), sep='\t')
        return url_file
    else:
        print('request failed')

def get_token():
    URL = 'https://www.homesalive.ca'
    endpoint = '/rest/V1/integration/admin/token'
    username = magento_config.username
    password = magento_config.password
    url = URL+endpoint
    header = {"Content-Type":"application/json"}
    payload = { "username": username, "password": password}
    response = requests.post(url, headers=header, data=json.dumps(payload))
    if response.status_code == 200:
        token_value = response.text.strip('"')
        print('Success')
        return token_value
    else: 
        print ('Error:', response.status_code)
        print (response.text)
        return None
# %%
product_endpoint = ("/rest/default/V1/products"
            "?searchCriteria[current_page]=1"
            "&searchCriteria[page_size]=1000"
            )
token = get_token()
product_url = 'https://www.homesalive.ca' + product_endpoint
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer "+token
}
response2 = requests.get(product_url, headers=headers)
if response2.status_code == 200:
    mag_products = response2.json()
    page_count = mag_products['total_count']
    total_page = -(-page_count//1000)
    print(f'Endpoint Success. Page Count: {total_page}')
else: 
    print ('Endpoint Error:', response2.status_code)
    print (response2.text)
# %%
mag_products = []
print('Initiating Loop')
for x in range (1, total_page + 1):
    product_endpoint = ("/rest/default/V1/products"
            f"?searchCriteria[current_page]={x}"
            "&searchCriteria[page_size]=1000"
            )
    product_url = 'https://www.homesalive.ca'+ product_endpoint
    headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer "+token }

    success = False
    retries_limit = 5

    while not success and retries_limit > 0:
        product_response = requests.get(product_url, headers=headers)
        if product_response.status_code == 503:
            print (f'Error 503 with page {x}, retrying.')
            time.sleep(60)
            retries_limit -= 1
        elif product_response.status_code == 401:
            print (f'Existing token expired on page {x}')
            token = get_token()
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer "+token }
        else: 
            try: 
                product_data = product_response.json()
                mag_products.extend(product_data['items'])
                print(f'Page: {x}')
                success = True
            except: 
                print(f'Json failed on page: {x}. Msg: {product_response.content}')
                retries_limit -= 1
    if not success:
        print (f'Data grab failed on page {x}')
df_mag_products = pd.DataFrame(mag_products)
#%%
storeid = ['3','4','7']
business_id= ['Homes Alive - St. Albert Trail','Homes Alive Pets - South Edmonton','Homes Alive Pets - West Edmonton']
store_name =['North','South','West']
testdf = []
for x, y, v in zip(storeid, business_id, store_name):
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.server};DATABASE={config.database};UID={config.username};PWD={config.password}'
    conn = pyodbc.connect(conn_string)
    sql_edmonton = f'''
    select itemlookupcode as id,
    case when getdate() between SnapShotSaleStartDate and SnapShotSaleEndDate then concat(SnapShotSalePrice, ' CAD')
    end as sale_price,
    case when getdate() between SnapShotSaleStartDate and SnapShotSaleEndDate then FORMAT(snapshotsalestartdate AT TIME ZONE 'Mountain Standard Time', 'yyyy-MM-ddTHH:mm:ss.fffzzz')
    end as sale_start_date,
    case when getdate() between SnapShotSaleStartDate and SnapShotSaleEndDate then FORMAT(snapshotsaleenddate AT TIME ZONE 'Mountain Standard Time', 'yyyy-MM-ddTHH:mm:ss.fffzzz')
    end as sale_end_date,
    case when snapshotquantity < 5 then 'limited availability'
    else 'in stock'
    end as availability
    from itemdynamic
    left join item 
    on itemdynamic.itemid = item.id 
    where storeid = {x}
    and snapshotquantity >= '2'

    '''
    edmonton_inventory = pd.read_sql(sql_edmonton, conn)
    edmonton_inventory['sale_price_effective_date'] = edmonton_inventory['sale_start_date'] + '/' +edmonton_inventory['sale_end_date'] 
    df_mag_products_r = df_mag_products.loc[df_mag_products['status']==1, ['sku','status','price']].copy()
    store_magento = pd.merge(edmonton_inventory, df_mag_products_r, 
                         left_on = 'id', right_on = 'sku', how = 'inner' )
    store_magento['price'] = store_magento['price'].astype(str) + ' CAD'
    store_magento['store_code'] = y
    store_magento['pickup_method'] = 'buy'
    store_magento['pickup_sla'] = 'same day'
    noffer = get_url_file('https://www.homesalive.ca/media/feeds/feed_1.txt')
    store_magento = store_magento[store_magento['id'].isin(noffer['id'].astype(str))]
    store_magento.to_csv(f'C:/Users/SolomonChang/Downloads/Python Scripts/Google-Merchant-Centre/{v} LIA.tsv', sep='\t', index=False)
    testdf.append(store_magento)

testdf2 = pd.concat(testdf, ignore_index = True)
conn.close()
# %%
repository = 'https://github.com/solomon789563/Google-Merchant-Centre'
local = 'C:\\Users\\SolomonChang\\Downloads\\Python Scripts\\Google-Merchant-Centre'
file = 'C:\\Users\SolomonChang\\Downloads\\Python Scripts\\Google-Merchant-Centre\\Edmonton LIA.tsv'
commit_message = 'Edmonton LIA file'
repo = Repo(local)
repo.index.add([file])
repo.index.commit(commit_message)
origin = repo.remote(name='origin')
origin.push()
# %%
