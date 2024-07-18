import pandas as pd
from faker import Faker
import random
import psycopg2
from config import config
from concurrent.futures import ThreadPoolExecutor
from crud_operations import (create_customer, read_customers,delete_customer, create_products, create_order_with_items,get_order_details,create_transactions_batch,update_transactions_batch, get_existing_ids,concurrent_create_customers,concurrent_create_products,concurrent_update_products,concurrent_create_orders_with_items,concurrent_update_orders_with_items,concurrent_create_transactions,concurrent_update_transactions)
import time
from datetime import datetime, timedelta



# Initialize the Faker object
fake = Faker()


# Database connection function
def get_db_connection():
    params = config()
    conn = psycopg2.connect(**params)
    return conn


# Generate fake data
def generate_customers(num):
    customers = []
    for _ in range(num):
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()[:20]
        address = fake.address()
        username = name.lower().replace(' ', '_')[:50]
        customers.append({
            #'name': name,
            'username': username,
            'email': email,
            'phone': phone,
            'address': address
        })
    return pd.DataFrame(customers)

# Generate 50 fake customers
#customers = generate_customers(50)
#print(customers)


def generate_products(num):
    categories = ['Sleeveless', 'T-shirt', 'Long-sleeve', 'Trouser', 'Shorts', 'Skirt']
    products = []
    
    for _ in range(num):
        name = fake.word().capitalize()  # Generate a random word and capitalize it
        category = random.choice(categories)
        price = round(random.uniform(100, 1200), 0)  
        quantity = random.randint(1, 50)  # Random quantity between 1 and 50
        products.append({
            'name': name,
            'category': category,
            'price': price,
            'quantity': quantity
        })
        #print(f"Generated product: {products}")
    
    return pd.DataFrame(products)

#products = generate_products(50)
#print(products)



#customer_ids = get_existing_ids('Customers', 'customer_id')
#product_ids = get_existing_ids('Products', 'product_id')

def generate_orders(num, customer_ids, product_ids):
    orders = []
    
    for _ in range(num):
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        quantity = random.randint(1, 10)  # Random quantity between 1 and 10
        
        # Fetch the price of the product from the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT price FROM Products WHERE product_id = %s', (product_id,))
        price = cursor.fetchone()[0] 
        cursor.close()
        conn.close()
        
        orders.append({
            'customer_id': customer_id,
            'product_id': product_id,
            'order_date': order_date,
            'quantity': quantity,
            'price': price * quantity
        })
    
    return pd.DataFrame(orders)

#orders = generate_orders(50, customer_ids, product_ids)

def generate_transactions(num, order_ids):
    transactions = []
    base_date = datetime.now()

    for i in range(num):
        # Fetch order details
        order_details = get_order_details(order_ids[i])
        if not order_details:
            print(f"Order details not found for order_id {order_ids[i]}")
            continue

        customer_id, product_id, price = order_details

        transaction = {
            'order_id': order_ids[i],
            'customer_id': customer_id,
            'product_id': product_id,
            'amount': price,  # Use the order price for each transaction
            'transaction_date': base_date - timedelta(days=random.randint(0, 365))  # Random date within the last year
        }
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)



if __name__ == "__main__":
    # Generate fake customers and products
    customers = generate_customers(10000)
    #products = generate_products(5000)

    # Get existing customer and product IDs from the database
    #customer_ids = get_existing_ids('Customers', 'customer_id')
    #product_ids = get_existing_ids('Products', 'product_id')
    #orders = generate_orders(6000, customer_ids, product_ids)
    #order_ids = get_existing_ids('Orders', 'order_id')
    #transactions = generate_transactions(4000, order_ids)

    # Measure performance without concurrency
    start_time = time.time()
    #delete_customer()
    # Perform CRUD operations without concurrency
    # For example:
    # - Insert customers
    #for _, row in customers.iterrows():
    #   create_customer(row.to_dict())
    
    
    # - Update customers
    # - Insert products
    #products_df = generate_products(3)  # Generate 3 products as a DataFrame
    #for _, row in products.iterrows():
    #    create_products(row.to_dict())
    # - Update products
    # - Generate orders and perform CRUD operations
    #for _, row in orders.iterrows():
     #    create_order_with_items(row.to_dict())
    #for _,row in transactions.iterrows():
    #    create_transaction(row.to_dict())

    end_time = time.time()
    execution_time_without_concurrency = end_time - start_time

    print(f"Execution time for sequentially creating(inserting) 10000 customers without concurrency: {execution_time_without_concurrency} seconds")

    #products = generate_products(5000)

    #transactions = generate_transactions(20000, order_ids)
    # Measure performance with concurrency
    #customers = generate_customers(10000)
    #orders = generate_orders(50000, customer_ids, product_ids)
    start_time = time.time()
    print(start_time)
    # Perform CRUD operations with concurrency
    #concurrent_create_customers(customers)
    #concurrent_update_customers(customers)
    #concurrent_create_products(products)
    #concurrent_update_products(products)

    #concurrent_create_transactions(transactions)

    # Generate orders and perform CRUD operations with items
    #concurrent_create_orders_with_items(orders)
    #concurrent_update_orders_with_items(orders) 

    end_time = time.time()
    execution_time_with_concurrency = end_time - start_time

    print(f"Execution time for creating(inserting) 10000 customers with concurrencyand batch: {execution_time_with_concurrency} seconds")
    #print(read_customers())