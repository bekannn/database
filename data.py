import pandas as pd
from faker import Faker
import random
import psycopg2
from config import config
from concurrent.futures import ThreadPoolExecutor
from crud_operations import (create_customer, create_product, create_order_with_items, get_existing_ids,concurrent_create_customers,concurrent_create_products,concurrent_update_products,concurrent_create_orders_with_items,concurrent_update_orders_with_items,concurrent_create_financial_metrics,concurrent_update_financial_metrics)
import time


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
        phone = fake.phone_number()
        address = fake.address()
        username = name.lower().replace(' ', '_')
        customers.append({
            'name': name,
            'email': email,
            'phone': phone,
            'address': address,
            'username': username
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
    
    return products

#products = generate_products(50)
#print(products)



customer_ids = get_existing_ids('Customers', 'customer_id')
product_ids = get_existing_ids('Products', 'product_id')

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
            'price': price
        })
    
    return orders

#orders = generate_orders(50, customer_ids, product_ids)


if __name__ == "__main__":
    # Generate fake customers and products
    customers = generate_customers(50)
    products = generate_products(80)

    # Get existing customer and product IDs from the database
    customer_ids = get_existing_ids('Customers', 'customer_id')
    product_ids = get_existing_ids('Products', 'product_id')
    orders = generate_orders(50, customer_ids, product_ids)

    # Measure performance without concurrency
    start_time = time.time()
    # Perform CRUD operations without concurrency
    # For example:
    # - Insert customers
    create_customer(customers)
    # - Update customers
    # - Insert products
    create_product(products)
    # - Update products
    # - Generate orders and perform CRUD operations
    create_order_with_items(orders)
    end_time = time.time()
    execution_time_without_concurrency = end_time - start_time

    print(f"Execution time without concurrency: {execution_time_without_concurrency} seconds")

    # Measure performance with concurrency
    start_time = time.time()
    # Perform CRUD operations with concurrency
    concurrent_create_customers(customers)
    #concurrent_update_customers(customers)
    concurrent_create_products(products)
    #concurrent_update_products(products)

    # Generate orders and perform CRUD operations with items
    orders = generate_orders(50, customer_ids, product_ids)
    concurrent_create_orders_with_items([(order, [{'product_id': order['product_id'], 'quantity': order['quantity'], 'price': order['price']}]) for order in orders])
    concurrent_update_orders_with_items([(order, [{'order_item_id': 1, 'product_id': order['product_id'], 'quantity': order['quantity'], 'price': order['price']}]) for order in orders])

    end_time = time.time()
    execution_time_with_concurrency = end_time - start_time

    print(f"Execution time with concurrency: {execution_time_with_concurrency} seconds")