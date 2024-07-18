import psycopg2
import pandas as pd
import time

# Connection configuration
params = {
    'dbname': 'mmaster',
    'user': 'postgres',
    'password': 'bhkm2547',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    return psycopg2.connect(**params)

def aggregate_query_per_customer(query):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=['customer_id', 'amount'])
        df['amount'] = df['amount'].astype(float)  # Convert to float

        # Perform aggregation per customer
        grouped = df.groupby('customer_id').agg(
            total_sum=('amount', 'sum'),
            overall_avg=('amount', 'mean'),
            overall_variance=('amount', 'var')
        )
        
        return grouped
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

# Example usage
query = "SELECT customer_id, amount FROM financial_transactions ORDER BY transaction_id"
star = time.time()
result = aggregate_query_per_customer(query)
#print(result)
end = time.time()
print(f"time= {end-star}")
