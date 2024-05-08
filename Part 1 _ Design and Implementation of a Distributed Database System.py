#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2

def connect_cockroach():
    try:
        connection = psycopg2.connect(
            dbname="Supplychain",
            user="postgres",
            password="Qwerty@99",
            host="localhost"
            port= 26257
        )
        print("Connection established successfully!")
        return connection
    except psycopg2.Error as e:
        print("Error while connecting to Cockroach:", e)
        return None


def create_tables():
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Create Product table
            cursor.execute('CREATE TABLE IF NOT EXISTS Product (product_id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, weight INT)')

            # Create Warehouse table
            cursor.execute('CREATE TABLE IF NOT EXISTS Warehouse (warehouse_id SERIAL PRIMARY KEY, location VARCHAR(100) NOT NULL, capacity INT,navigation VARCHAR(100))')

            # Create AvailableProduct table
            cursor.execute('CREATE TABLE IF NOT EXISTS AvailableProducts (warehouse_id INT REFERENCES Warehouse(warehouse_id), product_id INT REFERENCES Product(product_id), quantity INT, PRIMARY KEY (warehouse_id, product_id))')

            # Create Customer table
            cursor.execute('CREATE TABLE IF NOT EXISTS Customer (customer_id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, address VARCHAR(255), is_premium_customer BOOLEAN)')

            # Create Order details table
            cursor.execute('CREATE TABLE IF NOT EXISTS OrderDetails (order_id SERIAL PRIMARY KEY, customer_id INT REFERENCES Customer(customer_id), payment INT, date DATE, status VARCHAR(100))')

            # Create Orders table without order_id as primary key
            cursor.execute('CREATE TABLE IF NOT EXISTS Orders (order_id SERIAL, product_id INT REFERENCES Product(product_id))')

            # Delete data from Orders table first
            cursor.execute('DELETE FROM Orders')

            # Delete data from other tables
            cursor.execute('DELETE FROM AvailableProducts')
            cursor.execute('DELETE FROM Warehouse')
            cursor.execute('DELETE FROM Product')
            cursor.execute('DELETE FROM OrderDetails')
            cursor.execute('DELETE FROM Customer')

            conn.commit()
            print("Tables created and data deleted successfully!")

        except psycopg2.Error as e:
            print("Error while creating tables and deleting data:", e)

        finally:
            cursor.close()
            conn.close()
            
create_tables()

import random
from faker import Faker
import psycopg2



def insert_random_data():
    fake = Faker()

    # Valid product names
    valid_product_names = ["Laptop", "Smartphone", "Tablet", "Coffee Maker", "Bluetooth Speaker", "Headphones", "Digital Camera", "Printer", "Fitness Tracker", "External Hard Drive"]

    conn = connect_cockroach()
    
    if conn:
        try:
            cursor = conn.cursor()

            # Insert valid product names into Product table
            
            for product_name in valid_product_names:
                weight = random.randint(1, 1000)
                cursor.execute("INSERT INTO Product (name,weight) VALUES (%s , %s) RETURNING product_id", (product_name,weight))
                product_id = cursor.fetchone()[0]

                # Insert random data into Warehouse table with non-negative capacity and navigation
                capacity_value = random.randint(100, 500)
                navigation_value = random.choice(['North','South','East','West'])
                cursor.execute("INSERT INTO Warehouse (location, capacity, navigation) VALUES (%s, %s, %s) RETURNING warehouse_id", (fake.city(), capacity_value, navigation_value))
                warehouse_id = cursor.fetchone()[0]

                # Insert random data into AvailableProducts table with valid warehouse_id and product_id
                available_quantity = random.randint(0, 20)
                cursor.execute("INSERT INTO AvailableProducts (warehouse_id, product_id, quantity) VALUES (%s, %s, %s)", (warehouse_id, product_id, available_quantity))

                # Insert random data into Customer table with is_premium_customer status
                is_premium_customer = random.choice([True, False])
                cursor.execute("INSERT INTO Customer (name, address, is_premium_customer) VALUES (%s, %s, %s) RETURNING customer_id", (fake.name(), fake.address(), is_premium_customer))
                customer_id = cursor.fetchone()[0]

                # Insert random data into OrderDetails table with valid customer_id and status
                payment_amount = random.randint(50, 500)
                status_value = random.choice(["Processing", "Shipped", "Delivered"])
                cursor.execute("INSERT INTO OrderDetails (customer_id, payment, date, status) VALUES (%s, %s, %s, %s) RETURNING order_id", (customer_id, payment_amount, fake.date_between(start_date='-30d', end_date='today'), status_value))
                order_id = cursor.fetchone()[0]

                # Insert random data into Orders table with valid product_id
                cursor.execute("INSERT INTO Orders (order_id, product_id) VALUES (%s, %s)", (order_id, product_id))

            conn.commit()
            print("Data inserted successfully!")

        except psycopg2.Error as e:
            print("Error while inserting data:", e)

        finally:
            cursor.close()
            conn.close()

# Call the create_tables function to create tables and delete existing data
create_tables()

# Call the insert_random_data function to insert 15 rows of random data
insert_random_data()



def print_table_data(table_name):
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Fetch data from the specified table
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            # Print table data
            print(f"Table: {table_name}")
            for row in rows:
                print(row)

        except psycopg2.Error as e:
            print(f"Error while fetching data from {table_name}:", e)

        finally:
            cursor.close()
            conn.close()

# Call the print_table_data function for each table
print_table_data("Product")
print_table_data("Warehouse")
print_table_data("AvailableProducts")
print_table_data("Customer")
print_table_data("OrderDetails")
print_table_data("Orders")



