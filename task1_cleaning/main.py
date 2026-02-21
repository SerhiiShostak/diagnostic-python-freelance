import csv
from pathlib import Path
import re
import datetime
from dateutil import parser
import dateparser
from decimal import Decimal
import json

input_path = Path('task1_cleaning/data/input.csv')
output_path = Path('task1_cleaning/out/clean.csv')
report_path = Path('task1_cleaning/out/report.json')

def name_formatter(name: str) -> str:
    if not name:
        return ""

    name = " ".join(name.split())
    return name

    
def phone_formatter(phone_number: str) -> str:
    phone_number = re.sub(r'\D', '', phone_number)
    if not phone_number:
        return ""
    if len(phone_number) == 10 and phone_number[0] == '0':
        phone_number = '+38' + phone_number
        return phone_number
    elif len(phone_number) == 12 and phone_number[0:3] == '380':
        phone_number = '+' + phone_number
        return phone_number

    return ""

def email_formatter(email: str) -> str:
    if not email:
        return ""
    email = email.strip()
    if re.match(r"^[^\s]*@[^\s]*\.[^\s]*$", email):
        return email
    
    return ""

def date_formatter(date: str) -> str:
    if not date:
        return ""
    date = date.strip()
    try:
        parsed_date = datetime.datetime.strptime(date)
    except:
        parsed_date = dateparser.parse(date, languages=['en', 'ru', 'uk'])

    if parsed_date:
        return parsed_date.strftime('%Y-%m-%d')
    
    return ""

def amount_formatter(amount: str) -> str:
    if not amount:
        return ""
    amount = re.sub(r'[^0-9.,]', '', amount)
    if amount:
        try:
            amount = amount.replace(',', '.').split('.')

            if len(amount) > 2:
                parsed_amount = ''.join(amount[:-1]) + '.' + amount[-1]
            else:
                parsed_amount = '.'.join(amount)

            return f"{Decimal(parsed_amount):.2f}"
        except:
            return ""
        
    return ""

def deduplicate_data(data: list):
    n = len(data)
    parent = list(range(n)) #[0,1,2,3,4,5,6]
    size = [1] * n
    seen_phone = {}
    seen_email = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
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

    # for i, v in groups.items():
    #     print(f"Group {i}: {[data[i]['name'] for i in v]}")

    def get_key(index):
        d = data[index]['created_at']
        missing = 1 if not d else 0
        d_for_sort = datetime.date.max if d is None else d
        return (missing, d_for_sort, index)
    
    get_indices = []
    for root, indices in groups.items():
        best = min(indices, key=get_key)
        get_indices.append(best)
    
    get_indices.sort()
    deduplicated_rows = [data[i] for i in get_indices]

    return deduplicated_rows

with open(input_path, 'r') as f:
    reader = csv.DictReader(f)
    formatted_data = []
    report_dict = {
        "rows_in": 0, 
        "rows_out": 0,
        "dropped_empty_rows": 0,
        "invalid_phones": 0,
        "invalid_emails": 0,
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

    report_dict["rows_out"] = len(formatted_data)
    for person in formatted_data:
        if not person['phone']:
            report_dict['invalid_phones'] += 1
        if not person['email']:
            report_dict['invalid_emails'] += 1
        if not person['amount']:
            report_dict['invalid_amounts'] += 1

    final_data = deduplicate_data(formatted_data)
    report_dict["duplicates_removed"] = len(formatted_data) - len(final_data)
    report_dict["rows_out"] = len(final_data)

with open(output_path, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['lead_id', 'name', 'phone', 'email', 'created_at', 'amount'])
    writer.writeheader()
    writer.writerows(final_data)

with open(report_path, 'w') as f:
    json.dump(report_dict, f)
        
