from pathlib import Path
import argparse
import csv
import json
import logging
import time
from datetime import datetime
from collections import defaultdict

import requests
from requests import exceptions as rex

POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
USERS_URL = "https://jsonplaceholder.typicode.com/users"
COMMENTS_URL = "https://jsonplaceholder.typicode.com/comments"


def create_session() -> requests.Session:

    return requests.Session()


def fetch_json(url: str, session: requests.Session, timeout: int = 10, retries: int = 3, sleep: int = 1):

    endpoint = {
        "url": url,
        "ok": False,
        "status_code": 0, 
        "retries_used": 0,
        "error": "",
    }

    last_error = ""

    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            endpoint["status_code"] = resp.status_code

            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                endpoint["ok"] = False
                endpoint["retries_used"] = attempt
                endpoint["error"] = f"http_{resp.status_code} {resp.reason}"
                return None, endpoint

            if resp.status_code == 429 or resp.status_code >= 500:
                raise rex.RequestException(f"retryable http_{resp.status_code} {resp.reason}")

            try:
                data = resp.json()
            except ValueError as e:
                raise rex.RequestException(f"invalid_json: {e}")

            endpoint["ok"] = True
            endpoint["retries_used"] = attempt
            endpoint["error"] = ""
            return data, endpoint

        except (rex.Timeout, rex.ConnectionError, rex.RequestException) as e:
            last_error = str(e)
            logging.warning("fetch_json attempt %d/%d failed for %s: %s",
                            attempt + 1, retries + 1, url, last_error)

            if attempt >= retries:
                endpoint["ok"] = False
                endpoint["retries_used"] = attempt
                endpoint["error"] = last_error
                return None, endpoint

            time.sleep(sleep * (attempt + 1))

    endpoint["error"] = last_error
    return None, endpoint


def main():
    BASE_DIR = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Task 2: Fetch + Normalize API data and write output + report.")
    parser.add_argument("--out_dir", type=Path, default=BASE_DIR / "out")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--sleep", type=int, default=1)

    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s | %(levelname)s]: %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
    )

    report = {
        "started_at": None,
        "finished_at": None,
        "duration_sec": None,
        "endpoints": [],
        "rows": {"posts": 0, "users": 0, "comments": 0, "posts_enriched": 0},
        "warnings": [],
    }

    started_at = datetime.now()
    report["started_at"] = started_at.isoformat(timespec="seconds")
    logging.info("Started at %s", report["started_at"])

    session = create_session()

    posts_json, posts_ep = fetch_json(POSTS_URL, session, timeout=args.timeout, retries=args.retries, sleep=args.sleep)
    users_json, users_ep = fetch_json(USERS_URL, session, timeout=args.timeout, retries=args.retries, sleep=args.sleep)
    comments_json, comments_ep = fetch_json(COMMENTS_URL, session, timeout=args.timeout, retries=args.retries, sleep=args.sleep)

    report["endpoints"].extend([posts_ep, users_ep, comments_ep])

    report["rows"]["posts"] = len(posts_json) if isinstance(posts_json, list) else 0
    report["rows"]["users"] = len(users_json) if isinstance(users_json, list) else 0
    report["rows"]["comments"] = len(comments_json) if isinstance(comments_json, list) else 0

    exit_code = 0
    if posts_json is None:
        report["warnings"].append("posts endpoint failed; output is empty")
        logging.error("Failed to fetch posts; output will be empty.")
        exit_code = 1

    if users_json is None:
        report["warnings"].append("users endpoint failed; user_name/user_email will be empty")

    if comments_json is None:
        report["warnings"].append("comments endpoint failed; comments_count will be 0")

    user_map = {}
    if isinstance(users_json, list):
        user_map = {u.get("id"): (u.get("name"), u.get("email")) for u in users_json}

    comment_counts = defaultdict(int)
    if isinstance(comments_json, list):
        for c in comments_json:
            pid = c.get("postId")
            if pid is not None:
                comment_counts[pid] += 1

    output_list = []
    if isinstance(posts_json, list):
        for p in posts_json:
            uid = p.get("userId")
            name, email = user_map.get(uid, (None, None))
            output_list.append({
                "post_id": p.get("id"),
                "title": p.get("title"),
                "user_id": uid,
                "user_name": name,
                "user_email": email,
                "comments_count": int(comment_counts.get(p.get("id"), 0)),
            })

    report["rows"]["posts_enriched"] = len(output_list)

    out_file = args.out_dir / f"output.{args.format}"
    fieldnames = ["post_id", "title", "user_id", "user_name", "user_email", "comments_count"]

    try:
        if args.format == "csv":
            with open(out_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(output_list)
        else:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(output_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error("Failed to write output file: %s", e)
        report["warnings"].append(f"failed to write output file: {e}")
        exit_code = max(exit_code, 1)

    finished_at = datetime.now()
    report["finished_at"] = finished_at.isoformat(timespec="seconds")
    report["duration_sec"] = (finished_at - started_at).total_seconds()

    report_file = args.out_dir / "report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logging.info("Finished at %s (duration %.3fs)", report["finished_at"], report["duration_sec"])
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()