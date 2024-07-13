import psycopg2
from config import config
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.DEBUG)

def get_db_connection():
    params = config()
    conn = psycopg2.connect(**params)
    return conn

# Create (Insert)
def create_customer(customer):
    username = customer['username']
    email = customer['email']
    phone = customer['phone']
    address = customer['address']    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO Customers (username, email, phone, address) VALUES (%s,%s,%s,%s)',
            (username, email, phone, address)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Customer added!")
    except Exception as e:
        print(f"Error adding Customer {username}: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_create_customers(customers):
    cus_dicts = customers.to_dict(orient='records')
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_customer, cus_dicts))


def update_customer(customer):
    customer_id, username, email, phone, address = customer
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'UPDATE Customer with the customer_id = %s, username = %s, email = %s, phone = %s, address = %s',
            (customer_id, username, email, phone, address)
        )
        conn.commit()
        print(f"Customer {customer_id} updated!")
    except Exception as e:
        print(f"Error updating customer {customer_id}: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_update_customers(customers):
    with ThreadPoolExecutor() as executor:
        executor.map(update_customer, customers)

# Read (Select)
def read_customers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Customers;')
    customers = cursor.fetchall()
    cursor.close()
    conn.close()
    #print(customers)
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

def create_product(product):
    logging.debug(f"Debugging product data: {product}")
    #print(f"Debugging product data: {product}")
    name = product['name']
    category = product['category']
    price = product['price']
    quantity = product['quantity']
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO Products (name, category, price, quantity) VALUES (%s, %s::product_choice, %s, %s)',
            (name, category, price, quantity)
        )
        conn.commit()
        logging.info(f"Product {name} added!")
    except Exception as e:
        logging.error(f"Error adding product {name}: {e}")
    finally:
        cursor.close()
        conn.close()

def update_product(product):
    product_id, name, category, price, quantity = product
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'UPDATE Products SET name = %s, category = %s, price = %s, quantity = %s WHERE product_id = %s',
            (name, category, price, quantity, product_id)
        )
        conn.commit()
        print(f"Product {product_id} updated!")
    except Exception as e:
        print(f"Error updating product {product_id}: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_create_products(products):
    product_dicts = products.to_dict(orient='records')  # Convert DataFrame rows to a list of dictionaries
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_product, product_dicts))

def concurrent_update_products(products):
    with ThreadPoolExecutor() as executor:
        executor.map(update_product, products)

def get_existing_ids(table, id_column):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table}")
    ids = cursor.fetchall()
    cursor.close()
    conn.close()
    return [id[0] for id in ids]

#####################################################################
# Order CRUD Operations

def create_order_with_items(order):
    customer_id = order['customer_id']
    product_id = order['product_id']
    order_date = order['order_date']
    quantity = order['quantity']
    price = order['price']
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Insert the order
        cursor.execute(
            'INSERT INTO Orders (customer_id, order_date, product_id, quantity, price) VALUES (%s, %s, %s, %s, %s) RETURNING order_id;',
            (customer_id, order_date,product_id, quantity, price)
        )
        order_id = cursor.fetchone()[0]

        # Commit the transaction
        conn.commit()
        print(f"Order {order_id} and its items added!")
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        print(f"Error adding order and its items: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_create_orders_with_items(orders_with_items):
    orders_dicts = orders_with_items.to_dict(orient='records')
    with ThreadPoolExecutor() as executor:
        list(executor.map(create_order_with_items, orders_dicts))

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

#####################################################################
# Financial Metrics CRUD Operations

def create_financial_metric(metric):
    metric_name, value, metric_date = metric
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO Financial_Metrics (metric_name, value, metric_date) VALUES (%s, %s, %s)',
            (metric_name, value, metric_date)
        )
        conn.commit()
        print(f"Financial metric {metric_name} added!")
    except Exception as e:
        print(f"Error adding financial metric {metric_name}: {e}")
    finally:
        cursor.close()
        conn.close()

def update_financial_metric(metric):
    metric_id, metric_name, value, metric_date = metric
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'UPDATE Financial_Metrics SET metric_name = %s, value = %s, metric_date = %s WHERE metric_id = %s',
            (metric_name, value, metric_date, metric_id)
        )
        conn.commit()
        print(f"Financial metric {metric_id} updated!")
    except Exception as e:
        print(f"Error updating financial metric {metric_id}: {e}")
    finally:
        cursor.close()
        conn.close()

def concurrent_create_financial_metrics(metrics):
    with ThreadPoolExecutor() as executor:
        executor.map(create_financial_metric, metrics)

def concurrent_update_financial_metrics(metrics):
    with ThreadPoolExecutor() as executor:
        executor.map(update_financial_metric, metrics)
