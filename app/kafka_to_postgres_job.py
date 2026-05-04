from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os


KAFKA_BROKER = 'kafka:9093'
KAFKA_TOPIC = 'mock_data_topic'

DB_URL = 'jdbc:postgresql://postgres_db:5432/BigData'
DB_USER = 'postgres'
DB_PASSWORD = 'mysecretpassword'

JARS_DIR = '/opt/flink/jars'
REQUIRED_JARS = [
    'flink-sql-connector-kafka-1.17.1.jar',
    'flink-connector-jdbc-3.1.0-1.17.jar',
    'postgresql-42.5.4.jar'
]


def get_jar_urls():
    return ';'.join([f'file://{JARS_DIR}/{jar}' for jar in REQUIRED_JARS])


def create_kafka_source(t_env):
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `id` INT,
            `customer_first_name` STRING,
            `customer_last_name` STRING,
            `customer_email` STRING,
            `customer_country` STRING,
            `customer_pet_type` STRING,
            `pet_category` STRING,
            `product_name` STRING,
            `product_category` STRING,
            `product_brand` STRING,
            `product_material` STRING,
            `store_name` STRING,
            `store_city` STRING,
            `store_country` STRING,
            `store_email` STRING,
            `sale_date` STRING,
            `sale_quantity` INT,
            `sale_total_price` DOUBLE,
            `source_file` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-postgres-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)


def create_postgres_sinks(t_env):
    jdbc_common = f"""
        'connector' = 'jdbc',
        'url' = '{DB_URL}',
        'username' = '{DB_USER}',
        'password' = '{DB_PASSWORD}'
    """

    t_env.execute_sql(f"""
        CREATE TABLE dim_customers (
            customer_email STRING,
            customer_first_name STRING,
            customer_last_name STRING,
            customer_country STRING,
            PRIMARY KEY (customer_email) NOT ENFORCED
        ) WITH (
            {jdbc_common},
            'table-name' = 'dim_customers'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_products (
            product_name STRING,
            product_category STRING,
            product_brand STRING,
            product_material STRING,
            PRIMARY KEY (product_name) NOT ENFORCED
        ) WITH (
            {jdbc_common},
            'table-name' = 'dim_products'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_stores (
            store_email STRING,
            store_name STRING,
            store_city STRING,
            store_country STRING,
            PRIMARY KEY (store_email) NOT ENFORCED
        ) WITH (
            {jdbc_common},
            'table-name' = 'dim_stores'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_pets (
            pet_key STRING,
            pet_category STRING,
            customer_pet_type STRING,
            PRIMARY KEY (pet_key) NOT ENFORCED
        ) WITH (
            {jdbc_common},
            'table-name' = 'dim_pets'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fact_sales (
            original_id INT,
            customer_email STRING,
            product_name STRING,
            store_email STRING,
            pet_key STRING,
            sale_date STRING,
            sale_quantity INT,
            sale_total_price DOUBLE,
            source_file STRING,
            PRIMARY KEY (original_id, source_file) NOT ENFORCED
        ) WITH (
            {jdbc_common},
            'table-name' = 'fact_sales'
        )
    """)


def start_pipelines(t_env):
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO dim_customers
        SELECT DISTINCT
            customer_email,
            customer_first_name,
            customer_last_name,
            customer_country
        FROM kafka_source
        WHERE customer_email IS NOT NULL AND customer_email <> ''
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_products
        SELECT DISTINCT
            product_name,
            product_category,
            product_brand,
            product_material
        FROM kafka_source
        WHERE product_name IS NOT NULL AND product_name <> ''
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_stores
        SELECT DISTINCT
            store_email,
            store_name,
            store_city,
            store_country
        FROM kafka_source
        WHERE store_email IS NOT NULL AND store_email <> ''
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_pets
        SELECT DISTINCT
            CONCAT(pet_category, '|', customer_pet_type) AS pet_key,
            pet_category,
            customer_pet_type
        FROM kafka_source
        WHERE pet_category IS NOT NULL AND pet_category <> ''
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO fact_sales
        SELECT
            id AS original_id,
            customer_email,
            product_name,
            store_email,
            CONCAT(pet_category, '|', customer_pet_type) AS pet_key,
            sale_date,
            sale_quantity,
            sale_total_price,
            source_file
        FROM kafka_source
    """)

    return stmt_set.execute()


def init_postgres_tables():
    import psycopg2
    conn = psycopg2.connect(
        host='postgres_db',
        port=5432,
        dbname='BigData',
        user=DB_USER,
        password=DB_PASSWORD
    )
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_email VARCHAR(200) PRIMARY KEY,
                customer_first_name VARCHAR(100),
                customer_last_name VARCHAR(100),
                customer_country VARCHAR(100)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_products (
                product_name VARCHAR(200) PRIMARY KEY,
                product_category VARCHAR(100),
                product_brand VARCHAR(100),
                product_material VARCHAR(100)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_stores (
                store_email VARCHAR(200) PRIMARY KEY,
                store_name VARCHAR(200),
                store_city VARCHAR(100),
                store_country VARCHAR(100)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_pets (
                pet_key VARCHAR(200) PRIMARY KEY,
                pet_category VARCHAR(100),
                customer_pet_type VARCHAR(100)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                original_id INT NOT NULL,
                customer_email VARCHAR(200) REFERENCES dim_customers(customer_email),
                product_name VARCHAR(200) REFERENCES dim_products(product_name),
                store_email VARCHAR(200) REFERENCES dim_stores(store_email),
                pet_key VARCHAR(200) REFERENCES dim_pets(pet_key),
                sale_date VARCHAR(50),
                sale_quantity INT,
                sale_total_price DOUBLE PRECISION,
                source_file VARCHAR(100) NOT NULL,
                PRIMARY KEY (original_id, source_file)
            )
        """)
        conn.commit()
    conn.close()


def main():
    init_postgres_tables()

    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

    t_env.get_config().set("pipeline.jars", get_jar_urls())
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "DROP")

    create_kafka_source(t_env)
    create_postgres_sinks(t_env)

    print(f"Запускаю Flink Streaming: {KAFKA_TOPIC} -> PostgreSQL (модель звезда)")

    result = start_pipelines(t_env)
    result.wait()

    print("Задание завершено")


if __name__ == '__main__':
    main()