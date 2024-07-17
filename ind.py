import time
import psycopg2

"""def create_index():
    conn = psycopg2.connect("dbname=mmaster user=postgres password=bhkm2547")
    cursor = conn.cursor()
    start_time = time.time()
    
    cursor.execute("CREATE INDEX idx_products_name ON Products (name);")
    #cursor.execute("CREATE INDEX idx_products_category ON Products (category);")
    cursor.execute("CREATE INDEX idx_products_price ON Products (price);")
    
    conn.commit()
    end_time = time.time()
    
    print(f"Index creation time: {end_time - start_time} seconds")
    
    cursor.close()
    conn.close() """

def query_with_index():
    conn = psycopg2.connect("dbname=mmaster user=postgres password=bhkm2547")
    cursor = conn.cursor()
    start_time = time.time()
    
    cursor.execute("SELECT * FROM Products WHERE category = 'T-shirt';")
    rows = cursor.fetchall()
    
    end_time = time.time()
    
    print(f"Query execution time with index: {end_time - start_time} seconds")
    print(f"Number of rows retrieved: {len(rows)}")
    
    cursor.close()
    conn.close()

# Create indexes and measure the time
#create_index()

# Perform a query and measure the time
query_with_index()
