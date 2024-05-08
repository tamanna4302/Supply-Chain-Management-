#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
                
                # Part 4 
                
                cursor.execute("BEGIN")

                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                cursor.execute("SAVEPOINT  checkpoint")
                
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
            
            #Part 4 
            cursor.execute("RELEASE SAVEPOINT checkpoint")
            cursor.execute("COMMIT”)
            
            cursor.close()
            conn.close()

# Call the create_tables function to create tables and delete existing data
create_tables()

# Call the insert_random_data function to insert 15 rows of random data
insert_random_data()



