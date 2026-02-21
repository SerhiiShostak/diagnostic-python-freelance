import csv
from pathlib import Path
import re
import datetime
import dateparser
from decimal import Decimal, InvalidOperation
import json
import argparse
import logging

BASE_DIR = Path(__file__).resolve().parent
# input_path = BASE_DIR / "data" / "input.csv"
# output_path = BASE_DIR / "out" / "clean.csv"
# report_path = BASE_DIR / "out" / "report.json"
# output_path.parent.mkdir(parents=True, exist_ok=True)

cli_parser = argparse.ArgumentParser()
cli_parser.add_argument("--input", type=Path, default=BASE_DIR / "data" / "input.csv")
cli_parser.add_argument("--output", type=Path, default=BASE_DIR / "out" / "clean.csv")
cli_parser.add_argument("--report", type=Path, default=BASE_DIR / "out" / "report.json")

args = cli_parser.parse_args()
args.output.parent.mkdir(parents=True, exist_ok=True)
args.report.parent.mkdir(parents=True, exist_ok=True)

console_log = logging.StreamHandler()
logging.basicConfig(
    handlers=[console_log], 
    level=logging.INFO, 
    format='[%(asctime)s | %(levelname)s]: %(message)s', 
    datefmt='%d.%m.%Y %H:%M:%S',
    )

def name_formatter(name: str) -> str:
    if not name or not name.strip():
        return ""

    name = " ".join(name.split())
    return name
    
def phone_formatter(phone_number: str) -> str:
    if not phone_number or not phone_number.strip():
        return ""
    
    phone_number = re.sub(r'\D', '', phone_number)
    if len(phone_number) == 10 and phone_number[0] == '0':
        phone_number = '+38' + phone_number
        return phone_number
    elif len(phone_number) == 12 and phone_number[0:3] == '380':
        phone_number = '+' + phone_number
        return phone_number

    return ""

def email_formatter(email: str) -> str:
    if not email or not email.strip():
        return ""
    email = email.strip().lower()
    if re.match(r"^[^\s]*@[^\s]*\.[^\s]*$", email):
        return email
    
    return ""

def date_formatter(date: str) -> str:
    if not date or not date.strip():
        return ""
    date = date.strip()
    try:
        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    except:
        parsed_date = dateparser.parse(date, languages=['en', 'ru', 'uk'])

    if parsed_date:
        return parsed_date.strftime('%Y-%m-%d')
    
    return ""

def amount_formatter(amount: str) -> str:
    if not amount or not amount.strip():
        return ""
    currency_remove = ('uah', 'грн')
    amount = amount.lower()
    for cur in currency_remove:
        amount = amount.replace(cur, '')
    if re.search(r'[a-zA-Z]', amount):
        return ""
    amount = re.sub(r'[^0-9.,]', '', amount)
    if amount:
        try:
            amount = amount.replace(',', '.').split('.')
            if len(amount) == 2 and amount[0] and len(amount[0]) <= 3 and len(amount[1]) == 3:
                parsed_amount = ''.join(amount)
            elif len(amount) > 2:
                parsed_amount = ''.join(amount[:-1]) + '.' + amount[-1]
            else:
                parsed_amount = '.'.join(amount)

            return f"{Decimal(parsed_amount):.2f}"
        except (ValueError, TypeError, InvalidOperation):
            return ""
        
    return ""

def deduplicate_data(data: list) -> list:
    n = len(data)
    parent = list(range(n))
    size = [1] * n
    seen_phone = {}
    seen_email = {}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int) -> int:
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if size[rx] < size[ry]:
            rx, ry = ry, rx
        parent[ry] = rx
        size[rx] += size[ry]

    for i, r in enumerate(data):
        phone = r['phone']
        email = r['email']

        if phone:
            if phone in seen_phone:
                union(i, seen_phone[phone])
            else:
                seen_phone[phone] = i
        
        if email:
            if email in seen_email:
                union(i, seen_email[email])
            else:
                seen_email[email] = i

    groups = {}

    for i in range(n):
        root = find(i)
        groups.setdefault(root, []).append(i)

    def get_key(i):
        d = data[i]["created_at"]
        if not d:
            return (1, "9999-12-31", i)
        return (0, d, i) 
    
    get_indices = []
    for root, indices in groups.items():
        best = min(indices, key=get_key)
        get_indices.append(best)
    
    get_indices.sort()
    deduplicated_rows = [data[i] for i in get_indices]

    return deduplicated_rows

with open(args.input, 'r') as f:
    logging.info(f"Reading {args.input} file")
    reader = csv.DictReader(f)
    formatted_data = []
    report_dict = {
        "rows_in": 0, 
        "rows_out": 0,
        "dropped_empty_rows": 0,
        "invalid_phones": 0,
        "invalid_emails": 0,
        "invalid_dates": 0,
        "invalid_amounts": 0,
        "duplicates_removed": 0,
        }
    
    for row in reader:
        report_dict["rows_in"] += 1
        if not row['lead_id'].split() and not row['name'].split() and not row['phone'].split() and not row['email'].split() and not row['created_at'].split() and not row['amount'].split():
            report_dict['dropped_empty_rows'] += 1
            continue

        formatted_data.append({
            'lead_id': row['lead_id'],
            'name': name_formatter(row['name']),
            'phone': phone_formatter(row['phone']),
            'email': email_formatter(row['email']),
            'created_at': date_formatter(row['created_at']),
            'amount': amount_formatter(row['amount'])
        })
    logging.info(f"{report_dict['dropped_empty_rows']} empty rows removed")

    report_dict["rows_out"] = len(formatted_data)
    for person in formatted_data:
        if not person['phone']:
            report_dict['invalid_phones'] += 1
        if not person['email']:
            report_dict['invalid_emails'] += 1
        if not person['created_at']:
            report_dict['invalid_dates'] += 1
        if not person['amount']:
            report_dict['invalid_amounts'] += 1
    logging.info(f"{report_dict['invalid_phones']} invalid phones")
    logging.info(f"{report_dict['invalid_emails']} invalid emails")
    logging.info(f"{report_dict['invalid_dates']} invalid dates")
    logging.info(f"{report_dict['invalid_amounts']} invalid amounts")

    final_data = deduplicate_data(formatted_data)
    report_dict["duplicates_removed"] = len(formatted_data) - len(final_data)
    report_dict["rows_out"] = len(final_data)
    logging.info(f"{report_dict['duplicates_removed']} duplicates removed")
    logging.info(f"{report_dict['rows_out']} rows left")

with open(args.output, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['lead_id', 'name', 'phone', 'email', 'created_at', 'amount'])
    writer.writeheader()
    writer.writerows(final_data)
logging.info(f"Cleaned data saved to {args.output}")

with open(args.report, 'w') as f:
    json.dump(report_dict, f)
logging.info(f"Report saved to {args.report}")
logging.info("Done")

