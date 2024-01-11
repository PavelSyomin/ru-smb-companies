#!/usr/bin/env python
# coding: utf-8

# In[1]:


from collections import namedtuple
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests


# In[2]:


HOST = "https://cloud-api.yandex.net/v1/"
TOKEN = "y0_AgAAAAAIsQlOAAo5zgAAAADoimgdHQle499aS-aVeG4ejIKf9W2g6ak"


# In[3]:


headers = {
    "Accept": "application/json",
    "Authorization": f"OAuth {TOKEN}",
    "Content-Type": "application/json",    
}


# # Create folders

# In[41]:


create_folder_api_path = "disk/resources"


# In[42]:


params = {
    "path": "/tax-service-opendata"
}
url = urljoin(HOST, create_folder_api_path)


# In[43]:


response = requests.put(url, headers=headers, params=params)
assert response.status_code == 201


# In[45]:


for folder_name in ("debtam", "paytax", "revexp", "rsmp", "rsmppp", "sshr"):
    folder_path = f"/tax-service-opendata/{folder_name}"
    params = dict(path=folder_path)
    url = urljoin(HOST, create_folder_api_path)
    resp = requests.put(url, headers=headers, params=params)
    assert resp.status_code == 201
    print(f"Created {folder_path}")
    time.sleep(1)


# # Upload data

# In[4]:


data_sources = {
    "debtam": "https://www.nalog.gov.ru/opendata/7707329152-debtam/",
    "paytax": "https://www.nalog.gov.ru/opendata/7707329152-paytax/",
    "revexp": "https://www.nalog.gov.ru/opendata/7707329152-revexp/",
    "rsmp": "https://www.nalog.gov.ru/opendata/7707329152-rsmp/",
    "rsmppp": "https://www.nalog.gov.ru/opendata/7707329152-rsmppp/",
    "sshr": "https://www.nalog.gov.ru/opendata/7707329152-sshr2019/",
}


# In[5]:


def parse_page(url):
    # Container for the result
    ParseResult = namedtuple("ParseResult", ["data", "schemas"])
    
    # Get the page source
    print(f"Scraping {url}")
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Cannot get the page")
        return None
    
    # Make the soup
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Get the table of interest
    table = soup.find("table", class_="border_table")
    if table is None:
        print("Cannot find table in data source")
        return None
    
    # Parse table
    rows = table("tr")    
    row_index = {}
    for row in rows:
        cells = row("td")
        if len(cells) != 3:
            continue
        
        number = cells[0].get_text(strip=True)
        if number.isdigit():
            number = int(number)
        else:
            print("Unexpected non-numeric row number")
            continue
        
        name = cells[1].get_text(strip=True)
        value = cells[2]
        
        row_index[number] = dict(name=name, value=value)
    
    # Check whether the table contains all necessary items
    expected_rows = {
        8: "Гиперссылка (URL) на набор",
        10: "Описание структуры набора данных",
        16: "Гиперссылки (URL) на предыдущие релизы набора данных",
        17: "Гиперссылки (URL) на предыдущие версии структуры набора данных",
    }
    for expected_number, expected_name in expected_rows.items():
        name = row_index.get(expected_number, {}).get("name")
        if name != expected_name:
            print("Something is wrong with data source: "
                  f"expected {expected_name} on position {expected_number}, found {name}")
            return None
    
    # Extract URLs of data files and schema files
    data_files, schema_files = [], []
    
    data_links = row_index[8]["value"]("a") + row_index[16]["value"]("a")
    for link in data_links:
        url = link.get("href")
        if url is not None:
            data_files.append(url)
    
    schema_links = row_index[10]["value"]("a") + row_index[17]["value"]("a")
    for link in schema_links:
        url = link.get("href")
        if url is not None:
            schema_files.append(url)
    
    print(f"Found {len(data_files)} data link(s) and {len(schema_files)} schema link(s)")
    
    return ParseResult(data_files, schema_files)


# In[6]:


def make_yd_dir(path):
    print(f"Trying to create {path} on Yandex Disk")
    
    create_folder_api_path = "disk/resources"
    headers = {
        "Accept": "application/json",
        "Authorization": f"OAuth {TOKEN}",
        "Content-Type": "application/json",    
    }
    params = {
        "path": path,
    }
    url = urljoin(HOST, create_folder_api_path)
    
    resp = requests.put(url, headers=headers, params=params)
    
    if resp.status_code == 201:
        print("Folder created")
    elif resp.status_code == 409:
        print("Folder exists")
    else:
        print("Error while creating folder, see details below")
        print(resp.json())
        print()


# In[7]:


def get_existing_files(path):
    print(f"Trying to get items list for {path}")
    result = []
    create_folder_api_path = "disk/resources"
    headers = {
        "Accept": "application/json",
        "Authorization": f"OAuth {TOKEN}",
        "Content-Type": "application/json",    
    }
    params = {
        "path": path,
        "fields": "_embedded.items.path,_embedded.items.type",
        "limit": 500,
    }
    url = urljoin(HOST, create_folder_api_path)
    
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        print("Cannot get path medatata, see error message below")
        print(resp.json())
        print()
        return result

    for item in resp.json().get("_embedded", {}).get("items", []):
        if item.get("type") == "file":
            result.append(item.get("path"))
    
    print(f"Found {len(result)} files")
    
    return result


# In[8]:


def check_existing(link, existing_files):
    # Checking by name (the last part of link or path)
    _, _, link_name = link.rpartition("/")
    for existing_file in existing_files:
        _, _, file_name = existing_file.rpartition("/")
        if link_name == file_name:
            return True
        
    return False


# In[9]:


def upload_file(file_url, path):
    UploadResult = namedtuple("UploadResult", ["url", "info"])
    print(f"Uploading {file_url} to {path}")
    
    _, _, file_name = file_url.rpartition("/")
    dest = f"{path}/{file_name}"
    
    upload_api_path = "disk/resources/upload"
    headers = {
        "Accept": "application/json",
        "Authorization": f"OAuth {TOKEN}",
        "Content-Type": "application/json",    
    }
    params = {
        "url": file_url,
        "path": dest,
    }
    url = urljoin(HOST, upload_api_path)
    
    resp = requests.post(url, headers=headers, params=params)
    if resp.status_code == 202:
        print("Upload task created successfully")
        info = resp.json().get("href")
    else:
        print("Error while creating upload task")
        print(resp.json())
        info = None
    
    result = UploadResult(file_url, info)
    
    return result    


# In[10]:


def check_status(info_url):
    if info_url is None:
        return "cannot_check"
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"OAuth {TOKEN}",
        "Content-Type": "application/json",    
    }
    
    try:
        resp = requests.get(info_url, headers=headers, timeout=5)
    except Exception:
        return "check_error"
    
    if resp.status_code == 200:
        return resp.json().get("status")
    else:
        return "check_error"
    


# In[11]:


def process_data_source(name, url):
    # Get links to files
    links = parse_page(url)
    if links is None:
        return None
    
    # Destination on Yandex Disk
    data_path = f"/tax-service-opendata/{name}/data"
    schemas_path = f"/tax-service-opendata/{name}/schemas"
    
    # Make dirs if not exist
    make_yd_dir(data_path)
    make_yd_dir(schemas_path)
    
    status = {}
    # Upload new data files
    existing_data_files = get_existing_files(data_path)
    for link in links.data:
        if check_existing(link, existing_data_files):
            continue
        result = upload_file(link, data_path)
        status[result.url] = result.info
    
    # Upload new schema files
    existing_schema_files = get_existing_files(schemas_path)
    for link in links.schemas:
        if check_existing(link, existing_schema_files):
            continue
        result = upload_file(link, schemas_path)
        status[result.url] = result.info
    
    return status


# In[14]:


stats = {}
for name, url in data_sources.items():
    result = process_data_source(name, url)
    stats.update(result)


# In[16]:


for url, info_url in stats.items():
    if info_url == "task_create_error":
        continue
    print(f"{url}: {check_status(info_url)}")

