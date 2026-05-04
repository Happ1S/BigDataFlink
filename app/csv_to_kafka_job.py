import csv
import json
import glob
import os
from kafka import KafkaProducer


def create_producer():
    return KafkaProducer(
        bootstrap_servers='kafka:9093',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def process_csv_file(producer, filepath, topic):
    filename = os.path.basename(filepath)
    rows_sent = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sale_total = row.get('sale_total_price') or row.get('product_price') or '0'
            sale_qty = row.get('sale_quantity') or row.get('product_quantity') or '0'
            record = {
                'id': int(row['id']),
                'customer_first_name': row.get('customer_first_name', ''),
                'customer_last_name': row.get('customer_last_name', ''),
                'customer_email': row.get('customer_email', ''),
                'customer_country': row.get('customer_country', ''),
                'customer_pet_type': row.get('customer_pet_type', ''),
                'pet_category': row.get('pet_category', ''),
                'product_name': row.get('product_name', ''),
                'product_category': row.get('product_category', ''),
                'product_brand': row.get('product_brand', ''),
                'product_material': row.get('product_material', ''),
                'store_name': row.get('store_name', ''),
                'store_city': row.get('store_city', ''),
                'store_country': row.get('store_country', ''),
                'store_email': row.get('store_email', ''),
                'sale_date': row.get('sale_date', ''),
                'sale_quantity': int(sale_qty),
                'sale_total_price': float(sale_total),
                'source_file': filename
            }
            producer.send(
                topic,
                key=str(record['id']),
                value=record
            )
            rows_sent += 1

    return rows_sent


def main():
    DATA_DIR = '/opt/flink/data'
    TOPIC = 'mock_data_topic'

    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))

    if not csv_files:
        print(f"Файлы mock_data_*.csv не найдены в {DATA_DIR}")
        return

    print(f"Найдено {len(csv_files)} файлов для обработки")

    producer = create_producer()
    total_rows = 0

    for filepath in csv_files:
        rows = process_csv_file(producer, filepath, TOPIC)
        total_rows += rows
        print(f"  {os.path.basename(filepath)}: отправлено {rows} строк")

    producer.flush()
    producer.close()

    print(f"Готово. Всего отправлено {total_rows} сообщений в топик '{TOPIC}'")


if __name__ == '__main__':
    main()