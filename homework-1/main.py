"""Скрипт для заполнения данными таблиц в БД Postgres."""
from csv import DictReader

import psycopg2
import os

conn_params = {
  "host": "localhost",
  "database": "north",
  "user": "postgres",
  "password": "2366247"
}

with psycopg2.connect(**conn_params) as conn:

    with conn.cursor() as cur1:
        with open(os.path.join('north_data', 'customers_data.csv'), newline='', encoding='UTF-8') as file:
            reader = DictReader(file)
            for dictionary in reader:
                cur1.execute("INSERT INTO customers VALUES (%s, %s, %s)", (
                    dictionary.get('customer_id'),
                    dictionary.get('company_name'),
                    dictionary.get('contact_name')
                    ))

    with conn.cursor() as cur2:
        with open(os.path.join('north_data', 'employees_data.csv'), newline='', encoding='UTF-8') as file:
            reader = DictReader(file)
            for dictionary in reader:
                cur2.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (
                    dictionary.get('employee_id'),
                    dictionary.get('first_name'),
                    dictionary.get('last_name'),
                    dictionary.get('title'),
                    dictionary.get('birth_date'),
                    dictionary.get('notes')
                ))

    with conn.cursor() as cur3:
        with open(os.path.join('north_data', 'orders_data.csv'), newline='', encoding='UTF-8') as file:
            reader = DictReader(file)
            for dictionary in reader:
                cur3.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (
                    dictionary.get('order_id'),
                    dictionary.get('customer_id'),
                    dictionary.get('employee_id'),
                    dictionary.get('order_date'),
                    dictionary.get('ship_city')
                ))

conn.close()
