from pathlib import Path
import argparse
import json
import requests
import logging
import csv
from urllib3.util.retry import Retry
from datetime import datetime

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

    try:
        posts_request = session.get(POSTS_URL, timeout=timeout)
        posts_json = posts_request.json()
        
    except requests.exceptions.RequestException as e:
        logging.warning(e)
    finally:
        posts_endpoint = {
            "url": POSTS_URL,
            "ok": posts_request.ok,
            "status_code": posts_request.status_code,
            "retries_used": len(posts_request.history),
            "error": posts_request.reason,
        }
        report_dict["endpoints"].append(posts_endpoint)
    
    try:
        users_request = session.get(USERS_URL, timeout=timeout)
        users_json = users_request.json()
    except requests.exceptions.RequestException as e:
        logging.warning(e)
    finally:
        users_endpoint = {
            "url": USERS_URL,
            "ok": users_request.ok,
            "status_code": users_request.status_code,
            "retries_used": len(users_request.history),
            "error": users_request.reason,
        }
        report_dict["endpoints"].append(users_endpoint)

    try:
        comments_request = session.get(COMMENTS_URL, timeout=timeout)
        comments_json = comments_request.json()
    except requests.exceptions.RequestException as e:
        logging.warning(e)
    finally:
        comments_endpoint = {
            "url": COMMENTS_URL,
            "ok": comments_request.ok,
            "status_code": comments_request.status_code,
            "retries_used": len(comments_request.history),
            "error": comments_request.reason,
        }
        report_dict["endpoints"].append(comments_endpoint)

    output_list = []
    for post in posts_json:
        post_enriched = {
            'post_id': post['id'],
            'title': post['title'],
            'user_id': post['userId'],
            'user_name': None,
            'user_email': None,
            'comments_COUNT': 0,
        }

        for user in users_json:
            if user['id'] == post['userId']:
                post_enriched['user_name'] = user['name']
                post_enriched['user_email'] = user['email']

        for comment in comments_json:
            if comment['postId'] == post['id']:
                post_enriched['comments_COUNT'] += 1

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
    report_dict["rows"]["posts"] = len(posts_json)
    report_dict["rows"]["users"] = len(users_json)
    report_dict["rows"]["comments"] = len(comments_json)
    report_dict["rows"]["posts_enriched"] = len(output_list)

    with open(args.out_dir / "report.json", 'w', encoding='utf-8') as f:
        json.dump(report_dict, f, indent=4)
    logging.info(f"Finished at {finished_at}")

if __name__ == '__main__':
    main()  