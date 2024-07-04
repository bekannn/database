import psycopg2
from config import config

def get_db_connection():
    params = config()
    conn = psycopg2.connect(**param)
    return conn

# Create (Insert)
def creat_customer(username, email, phone, address):
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
    with ThreadPoolExecutor() as executor:
        executor.map(create_customer, customers)


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
    return customers

# Delete
def delete_customer(customer_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Customers WHERE customer_id = %s', (customer_id,))
    conn.commit()
    cursor.close()
    conn.close()
    print("Customer deleted!")