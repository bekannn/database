import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time

# Connection pool configuration
params = {
    'dbname': 'mmaster',
    'user': 'postgres',
    'password': 'bhkm2547',
    'host': 'localhost',
    'port': '5432'
}

conn_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **params)

def get_db_connection():
    return conn_pool.getconn()

def release_db_connection(conn):
    conn_pool.putconn(conn)

def get_unique_customers(query):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT customer_id FROM ({query}) AS subquery")
    customer_ids = cursor.fetchall()
    cursor.close()
    release_db_connection(conn)
    return [id[0] for id in customer_ids]

def process_partition(query, customer_id_subset):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Split query to insert WHERE clause correctly
    if "ORDER BY" in query:
        select_part = query.split("ORDER BY")[0]
        order_by_part = "ORDER BY" + query.split("ORDER BY")[1]
    else:
        select_part = query
        order_by_part = ""

    customer_id_str = ','.join(map(str, customer_id_subset))
    modified_query = f"{select_part} WHERE customer_id IN ({customer_id_str}) {order_by_part}"
    cursor.execute(modified_query)
    rows = cursor.fetchall()
    cursor.close()
    release_db_connection(conn)
    
    df = pd.DataFrame(rows, columns=['customer_id', 'amount'])  # Adjust columns as per your query result
    df['amount'] = df['amount'].astype(float)  # Convert amount to float
    
    grouped = df.groupby('customer_id').agg(
        total_sum=('amount', 'sum'),
        overall_avg=('amount', 'mean'),
        overall_variance=('amount', 'var')
    )
    
    return grouped

def parallel_aggregate(query, num_partitions):
    customer_ids = get_unique_customers(query)
    partition_size = len(customer_ids) // num_partitions
    customer_id_partitions = [customer_ids[i:i + partition_size] for i in range(0, len(customer_ids), partition_size)]
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_partition, query, partition) for partition in customer_id_partitions]
    
    aggregated_results = pd.DataFrame()
    for future in futures:
        partition_result = future.result()
        aggregated_results = pd.concat([aggregated_results, partition_result])
    
    return aggregated_results

query = "SELECT customer_id, amount FROM financial_transactions ORDER BY transaction_id"
start_time = time.time()
result = parallel_aggregate(query, num_partitions=10)
#print(result)
end_time = time.time()
print(f"Time = {end_time - start_time}")
