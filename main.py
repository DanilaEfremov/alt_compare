import json
import os.path
from pathlib import Path

import requests
import pandas as pd

BASE_DIR = str(Path.home())
CACHE_DIR = '.sisyphus'
TTL = 60 * 60 * 4 # время в секундах в течении которого кешированный файл считается неустаревшим

def download_json_file(dir_path, file_name):
    if os.path.exists(f"{BASE_DIR}/{dir_path}/{file_name}.json"):
        print("Already exists")
    else:
        r = requests.get(f"https://rdb.altlinux.org/api/export/branch_binary_packages/{file_name}")
        sisyphus = r.json()
        os.makedirs(f"{BASE_DIR}/{dir_path}", exist_ok=True)
        my_file = open(
            f"{BASE_DIR}/{dir_path}/{file_name}.json", "w+", encoding="utf-8"
        )
        json.dump(sisyphus, my_file, indent=2, ensure_ascii=False)
        my_file.close()
        print(f"Downloaded to {BASE_DIR}/{dir_path}/{file_name}.json")


def main():
    branch_sisyphus = 'sisyphus'
    branch_p11 = 'p11'
    download_json_file(CACHE_DIR, branch_sisyphus)
    download_json_file(CACHE_DIR, branch_p11)

    with open(f"{BASE_DIR}/{CACHE_DIR}/{branch_sisyphus}.json", "r", encoding="utf-8") as f:
        sisyphus = json.load(f)['packages']

    with open(f"{BASE_DIR}/{CACHE_DIR}/{branch_p11}.json", "r", encoding="utf-8") as f:
        p11 = json.load(f)['packages']

if __name__ == "__main__":
    main()