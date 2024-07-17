import psycopg2
from config import config
from concurrent.futures import ThreadPoolExecutor
import logging
from psycopg2 import pool
import psycopg2.extras


# Configure logging
logging.basicConfig(level=logging.INFO)

params = {
    'dbname': 'mmaster',
    'user': 'postgres',
    'password': 'bhkm2547',
    'host': 'localhost',
    'port': '5432'
}

'''def get_db_connection():
    params = config()
    conn = psycopg2.connect(**params)
    return conn '''
conn_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **params)

def get_db_connection():
    return conn_pool.getconn()

def release_db_connection(conn):
    conn_pool.putconn(conn)

# Create (Insert) 

def create_customer(customer):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO Customers (username, email, phone, address) VALUES (%s, %s, %s, %s)',
            (customer['username'], customer['email'], customer['phone'], customer['address'])
        )
        conn.commit()
        logging.info(f"Customer {customer['username']} added!")
    except Exception as e:
        logging.error(f"Error adding customer {customer['username']}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_create_customers(customers):
    cus_dicts = customers.to_dict(orient='records')
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_customer, cus_dicts))

def convert_to_tuples(records, columns):
    return [tuple(record[col] for col in columns) for record in records]

"""



# Create (Insert) with Batch Inserts
def create_customers_batch(customers_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Convert list of dicts to list of tuples
        customers_tuples = [(customer['username'], customer['email'], customer['phone'], customer['address']) for customer in customers_batch]
        
        insert_query = 'INSERT INTO Customers (username, email, phone, address) VALUES %s'
        psycopg2.extras.execute_values(cursor, insert_query, customers_tuples)
        #use execute_values for bulk insertion
        conn.commit()
        logging.info(f"Batch of {len(customers_batch)} customers added!")
    except Exception as e:
        logging.error(f"Error adding batch of customers: {e}")
        conn.rollback() # If there is an error, roll back to maintain the consistency of the data
    finally:
        cursor.close()
        release_db_connection(conn) # release the connection back to the connection pool

def concurrent_create_customers(customers, batch_size=100):
    cus_dicts = customers.to_dict(orient='records')
    # Create batches
    customer_batches = [cus_dicts[i:i + batch_size] for i in range(0, len(cus_dicts), batch_size)]
    
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_customers_batch, customer_batches))
        """

# Update (Batch Updates) for Customers
def update_customers(customers_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        update_query = 'UPDATE Customers SET username = data.username, email = data.email, phone = data.phone, address = data.address FROM (VALUES %s) AS data (customer_id, username, email, phone, address) WHERE Customers.customer_id = data.customer_id'
        customers_tuples = convert_to_tuples(customers_batch, ['customer_id', 'username', 'email', 'phone', 'address'])
        psycopg2.extras.execute_values(cursor, update_query, customers_tuples)
        conn.commit()
        logging.info(f"Batch of {len(customers_batch)} customers updated!")
    except Exception as e:
        logging.error(f"Error updating batch of customers: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_update_customers(customers, batch_size=100):
    customer_batches = [customers[i:i + batch_size] for i in range(0, len(customers), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(update_customers, customer_batches))
# Read (Select)
def read_customers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Customers;')
    customers = cursor.fetchall()
    cursor.close()
    conn.close()
    release_db_connection(conn)
    return customers

# Delete
def delete_customer(customer_id=None, column=None, value=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if customer_id is not None:
        # Delete a single customer by ID
        cursor.execute('DELETE FROM Customers WHERE customer_id = %s', (customer_id,))
        print(f"Customer with ID {customer_id} deleted!")
    elif column is not None and value is not None:
        # Delete customers based on specific column and value
        query = f'DELETE FROM Customers WHERE {column} = %s'
        cursor.execute(query, (value,))
        print(f"Customers with {column} = {value} deleted!")
    else:
        # Delete all customers
        cursor.execute('DELETE FROM Customers')
        print("All customers deleted!")
    
    conn.commit()
    cursor.close()
    conn.close()


#####################################################################
# Product CRUD Operations

# Create (Insert) with Batch Inserts for Products
def create_products(products_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        products_tuples = convert_to_tuples(products_batch, ['name', 'category', 'price', 'quantity'])
        insert_query = 'INSERT INTO Products (name, category, price, quantity) VALUES %s'
        psycopg2.extras.execute_values(cursor, insert_query, products_tuples)
        conn.commit()
        logging.info(f"Batch of {len(products_batch)} products added!")
    except Exception as e:
        logging.error(f"Error adding batch of products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_create_products(products, batch_size=100):
    product_dicts = products.to_dict(orient='records')
    product_batches = [product_dicts[i:i + batch_size] for i in range(0, len(product_dicts), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_products, product_batches))

# Update (Batch Updates) for Products
def update_products(products_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        update_query = 'UPDATE Products SET name = data.name, category = data.category, price = data.price, quantity = data.quantity FROM (VALUES %s) AS data (product_id, name, category, price, quantity) WHERE Products.product_id = data.product_id'
        products_tuples = convert_to_tuples(products_batch, ['product_id', 'name', 'category', 'price', 'quantity'])
        psycopg2.extras.execute_values(cursor, update_query, products_tuples)
        conn.commit()
        logging.info(f"Batch of {len(products_batch)} products updated!")
    except Exception as e:
        logging.error(f"Error updating batch of products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_update_products(products, batch_size=100):
    product_batches = [products[i:i + batch_size] for i in range(0, len(products), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(update_products, product_batches))

def get_existing_ids(table, id_column):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table}")
    ids = cursor.fetchall()
    cursor.close()
    release_db_connection(conn)
    return [id[0] for id in ids]

#####################################################################
# Order CRUD Operations

def create_order_with_items(orders_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Insert the order
        order_tup = convert_to_tuples(orders_batch,['customer_id', 'order_date','product_id', 'quantity', 'price'] )
        query = 'INSERT INTO Orders (customer_id, order_date, product_id, quantity, price) VALUES %s RETURNING order_id;'
        psycopg2.extras.execute_values(cursor, query, order_tup)
        logging.info(f"Batch of {len(orders_batch)} products added!")
        conn.commit()
    except Exception as e:
        logging.error(f"Error adding batch of products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)


def concurrent_create_orders_with_items(orders_with_items, batch_size=100):
    orders_dicts = orders_with_items.to_dict(orient='records')
    order_batch = [orders_dicts[i:i + batch_size] for i in range(0,len(orders_dicts), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_order_with_items, order_batch))

    

def update_order_with_items(order, order_items):
    order_id, customer_id, order_date = order
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        
        # Update the order
        cursor.execute(
            'UPDATE Orders SET customer_id = %s, order_date = %s WHERE order_id = %s',
            (customer_id, order_date, order_id)
        )

        # Update each order item
        for item in order_items:
            order_item_id, product_id, quantity, price = item
            cursor.execute(
                'UPDATE OrderItems SET product_id = %s, quantity = %s, price = %s WHERE order_item_id = %s',
                (product_id, quantity, price, order_item_id)
            )

        # Commit the transaction
        conn.commit()
        print(f"Order {order_id} and its items updated!")
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        print(f"Error updating order and its items: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_update_orders_with_items(orders_with_items):
    with ThreadPoolExecutor() as executor:
        executor.map(lambda args: update_order_with_items(*args), orders_with_items)

def get_order_details(order_id):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT customer_id, product_id, price FROM Orders WHERE order_id = %s', (order_id,)
        )
        order_details = cursor.fetchone()
        cursor.close()
        return order_details
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error fetching order details for order_id {order_id}: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()


#####################################################################
# Financial Metrics CRUD Operations

# Create (Insert) with Batch Inserts for Financial Transactions
def create_transactions_batch(transactions_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        transactions_tuples = convert_to_tuples(transactions_batch, ['order_id', 'customer_id', 'product_id', 'amount', 'transaction_date'])
        insert_query = 'INSERT INTO Financial_Transactions (order_id, customer_id, product_id, amount, transaction_date) VALUES %s'
        psycopg2.extras.execute_values(cursor, insert_query, transactions_tuples)
        conn.commit()
        logging.info(f"Batch of {len(transactions_batch)} transactions added!")
    except Exception as e:
        logging.error(f"Error adding batch of transactions: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_create_transactions(transactions, batch_size=100):
    transactions_dicts = transactions.to_dict(orient='records')
    transaction_batches = [transactions_dicts[i:i + batch_size] for i in range(0, len(transactions_dicts), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_transactions_batch, transaction_batches))

# Update (Batch Updates) for Financial Transactions
def update_transactions_batch(transactions_batch):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        update_query = 'UPDATE Financial_Transactions SET customer_id = data.customer_id, product_id = data.product_id, amount = data.amount, transaction_date = data.transaction_date FROM (VALUES %s) AS data (transaction_id, order_id, customer_id, product_id, amount, transaction_date) WHERE Financial_Transactions.transaction_id = data.transaction_id'
        transactions_tuples = convert_to_tuples(transactions_batch, ['transaction_id', 'order_id', 'customer_id', 'product_id', 'amount', 'transaction_date'])
        psycopg2.extras.execute_values(cursor, update_query, transactions_tuples)
        conn.commit()
        logging.info(f"Batch of {len(transactions_batch)} transactions updated!")
    except Exception as e:
        logging.error(f"Error updating batch of transactions: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_db_connection(conn)

def concurrent_update_transactions(transactions, batch_size=100):
    transaction_batches = [transactions[i:i + batch_size] for i in range(0, len(transactions), batch_size)]
    with ThreadPoolExecutor() as executor:
        list(executor.map(update_transactions_batch, transaction_batches))