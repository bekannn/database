import time
import psycopg2
from config import config
from psycopg2 import pool
import concurrent.futures


def get_db_connection():
    params = config()
    conn = psycopg2.connect(**params)
    return conn

def aggregate_financial_metrics():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT customer_id, SUM(amount) AS total_amount, AVG(amount) AS avg_amount
    FROM Financial_Transactions
    GROUP BY customer_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

start_time = time.time()
financial_metrics = aggregate_financial_metrics()
end_time = time.time()

print(f"Original Code Execution Time: {end_time - start_time} seconds")
#for metric in financial_metrics:
 #   print(f"Customer ID: {metric[0]}, Total Amount: {metric[1]}, Average Amount: {metric[2]}")

#---------------------------------------------------------------------------------

#params = config()
#conn_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **params)

#def get_db_connection_pool():
 #   return conn_pool.getconn()

#def release_db_connection(conn):
 #   if conn_pool:
  #      conn_pool.putconn(conn)

def aggregate_financial_metrics_batch(batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = f"""
    SELECT customer_id, SUM(amount) AS total_amount, AVG(amount) AS avg_amount
    FROM Financial_Transactions
    WHERE customer_id IN {batch}
    GROUP BY customer_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    #release_db_connection(conn)
    return results

def get_batches(customers, batch_size):
    for i in range(0, len(customers), batch_size):
        yield tuple(customers[i:i + batch_size])

def aggregate_financial_metrics_parallel(customers, batch_size=100):
    batches = list(get_batches(customers, batch_size))
    all_results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_batch = {executor.submit(aggregate_financial_metrics_batch, batch): batch for batch in batches}
        for future in concurrent.futures.as_completed(future_to_batch):
            all_results.extend(future.result())
    
    return all_results

def get_existing_ids(table, id_column):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table}")
    ids = cursor.fetchall()
    cursor.close()
    conn.close()
    return [id[0] for id in ids]

# Example list of customer IDs
customer_ids = get_existing_ids('Customers', 'customer_id')  # Replace with your actual customer IDs

start_time = time.time()
financial_metrics = aggregate_financial_metrics_parallel(customer_ids)
end_time = time.time()

print(f"Optimized Code Execution Time: {end_time - start_time} seconds")
#for metric in financial_metrics:
 #   print(f"Customer ID: {metric[0]}, Total Amount: {metric[1]}, Average Amount: {metric[2]}")

batch_sizes = [10, 50, 100, 500, 1000]  # Different batch sizes to test
results = {}

for batch_size in batch_sizes:
    start_time = time.time()
    financial_metrics = aggregate_financial_metrics_parallel(customer_ids, batch_size=batch_size)
    end_time = time.time()
    execution_time = end_time - start_time
    results[batch_size] = execution_time
    print(f"Batch Size: {batch_size}, Execution Time: {execution_time} seconds")

optimal_batch_size = min(results, key=results.get)
print(f"Optimal Batch Size: {optimal_batch_size}, Execution Time: {results[optimal_batch_size]} seconds")