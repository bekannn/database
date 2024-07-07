import pandas as pd
from faker import Faker
import random
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from crud_operations import create_customer, create_product, create_order, get_existing_ids

# Initialize the Faker object
fake = Faker()


# Database connection function
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="your_database_name",
        user="your_username",
        password="your_password"
    )


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
customers = generate_customers(50)
print(customers)


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

products = generate_products(50)
print(products)



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

# Example usage
orders = generate_orders(50, customer_ids, product_ids)
for order in orders:
    print(order)
