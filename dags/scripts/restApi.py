import os
from pathlib import Path
from typing import List
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

def put_key(items: List[dict], logical_date: str) -> List[dict]:
    res = []
    for i in items:
        item = {}
        item["id"] = i["id"]
        item["name"] = i["name"]
        item["stars"] = i["stargazers_count"]
        item["watchers"] = i["watchers_count"]
        item["forks"] = i["forks_count"]
        item["owner_login"] = i["owner"]["login"]
        item["created_at"] = logical_date
        res.append(item)
    return res

def get_repo_order_stars(logical_date: str, per_page: int = 10) -> List[dict]:
    TOKEN = os.getenv("GITHUB_TOKEN")
    # HEADERS = {"Authorization": f"Bearer {TOKEN}"}

    url = f"https://api.github.com/search/repositories?q=stars:>0&sort=stars&order=desc&page=1&per_page={per_page}"
    response = requests.get(url
                            # , headers=HEADERS
                            )
    if response.status_code == 200:
        data = response.json()
        return put_key(data["items"], logical_date)
    else:
        print(str(response.status_code))
        response.raise_for_status() #хз

