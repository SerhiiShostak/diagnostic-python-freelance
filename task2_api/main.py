from pathlib import Path
import argparse
import json
import requests
import logging
import csv
from urllib3.util.retry import Retry
from datetime import datetime
import time

POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
USERS_URL = "https://jsonplaceholder.typicode.com/users"
COMMENTS_URL = "https://jsonplaceholder.typicode.com/comments"
class FixedDelay(Retry):
    def get_backoff_time(self):
        return self.backoff_factor

def create_session(retries=3, backoff_factor=1):
    session = requests.Session()
    retry_strategy = FixedDelay(
        total=retries, 
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET', 'POST'],
        raise_on_status=False,
        )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_json(url, session, timeout=10, retries=3, sleep=1):
    endpoint = {
        "url": url,
        "ok": False,
        "status_code": 0,
        "retries_used": 0,
        "error": "",
    }
    
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            endpoint["status_code"] = response.status_code
            if 400 <= response.status_code < 500 and response.status_code != 429:
                endpoint["ok"] = False
                endpoint["retries_used"] = attempt
                endpoint["error"] = response.reason

                return None, endpoint
            
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.exceptions.RequestException(f"Retryable: {response.status_code} {response.reason}")

            try:
                data = response.json()
            except ValueError as e:
                raise requests.exceptions.RequestException(e)
            
            endpoint["ok"] = True
            endpoint["retries_used"] = attempt
            endpoint["error"] = ""

            return data, endpoint
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
            logging.warning(f"fetch_json attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt >= retries:
                endpoint["ok"] = False
                endpoint["retries_used"] = attempt
                endpoint["error"] = str(e)

                return None, endpoint
            
            time.sleep(sleep * (attempt + 1))

    return None, endpoint
    # try:
    #     posts_request = session.get(POSTS_URL, timeout=timeout)
    #     posts_json = posts_request.json()
        
    # except requests.exceptions.RequestException as e:
    #     logging.warning(e)
    #     report_dict["warnings"].append(e)
    # finally:
    #     posts_endpoint = {
    #         "url": POSTS_URL,
    #         "ok": posts_request.ok,
    #         "status_code": posts_request.status_code,
    #         "retries_used": len(posts_request.history),
    #         "error": posts_request.reason,
    #     }
    #     report_dict["endpoints"].append(posts_endpoint)


def main():
    BASE_DIR = Path(__file__).resolve().parent

    cli_parser = argparse.ArgumentParser()
    default_file_format = 'csv'
    cli_parser.add_argument("--out_dir", type=Path, default=BASE_DIR / "out")
    cli_parser.add_argument("--format", type=str, default=default_file_format)
    cli_parser.add_argument("--timeout", type=str, default='10')
    cli_parser.add_argument("--retries", type=str, default='3')
    cli_parser.add_argument("--sleep", type=str, default='1')

    args = cli_parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    file_format = args.format

    out_dir = args.out_dir / f"output.{file_format}"

    timeout = int(args.timeout)
    retries = int(args.retries)
    sleep = int(args.sleep)
    report_dict = {
        "started_at": None,
        "finished_at": None,
        "duration_sec": None,
        "endpoints": [],
        "rows": {
            "posts": 0,
            "users": 0,
            "comments": 0,
            "posts_enriched": 0,
            },
        "warnings": [],
    }

    console_log = logging.StreamHandler()
    logging.basicConfig(
        handlers=[console_log],
        level=logging.DEBUG,
        format='[%(asctime)s | %(levelname)s]: %(message)s', 
        datefmt='%d.%m.%Y %H:%M:%S',
    )

    started_at = datetime.now()
    logging.info(f"Started at {started_at}")
    report_dict["started_at"] = started_at.strftime('%d.%m.%Y %H:%M:%S')
    session = create_session(retries=retries, backoff_factor=sleep)

    posts_json, posts_endpoint = fetch_json(POSTS_URL, session, timeout=timeout, retries=retries, sleep=sleep)
    report_dict["endpoints"].append(posts_endpoint)

    users_json, users_endpoint = fetch_json(USERS_URL, session, timeout=timeout, retries=retries, sleep=sleep)
    report_dict["endpoints"].append(users_endpoint)

    comments_json, comments_endpoint = fetch_json(COMMENTS_URL, session, timeout=timeout, retries=retries, sleep=sleep)
    report_dict["endpoints"].append(comments_endpoint)
    post_enriched = {}
    output_list = []
    if posts_json:
        for post in posts_json:
            post_enriched = {
                'post_id': post['id'],
                'title': post['title'],
                'user_id': post['userId'],
                'user_name': None,
                'user_email': None,
                'comments_count': 0,
            }

        if users_json:
            for user in users_json:
                if user.get('id') and user.get('id') == post['userId']:
                    post_enriched['user_name'] = user['name']
                    post_enriched['user_email'] = user['email']

        if comments_json:
            for comment in comments_json:
                if comment.get('postId') == post.get('id'):
                    post_enriched['comments_count'] += 1
    else:
        logging.error("Failed to fetch posts")
        output_list.append(post_enriched)


    output_list.append(post_enriched)
    try:
        if file_format == 'csv':
            with open(out_dir, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=output_list[0].keys())
                writer.writeheader()
                writer.writerows(output_list)
        elif file_format == 'json':
            with open(out_dir, 'w', encoding='utf-8') as f:
                json.dump(output_list, f)
        else:
            raise Exception("Unsupported file format")
    except Exception as e:
        logging.error("Failed to write output file")
        logging.error(e)

    finished_at = datetime.now()
    report_dict["finished_at"] = finished_at.strftime('%d.%m.%Y %H:%M:%S')
    report_dict["duration_sec"] = (finished_at - started_at).total_seconds()
    report_dict["rows"]["posts"] = len(posts_json) if posts_json else 0
    report_dict["rows"]["users"] = len(users_json) if users_json else 0
    report_dict["rows"]["comments"] = len(comments_json) if comments_json else 0
    report_dict["rows"]["posts_enriched"] = len(output_list) if posts_json else 0

    with open(args.out_dir / "report.json", 'w', encoding='utf-8') as f:
        json.dump(report_dict, f, indent=4)
    logging.info(f"Finished at {finished_at}")

if __name__ == '__main__':
    main()  