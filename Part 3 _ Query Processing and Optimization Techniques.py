#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Indexing on is_premium_customer in Customer table:

def create_index_on_premium_customer(conn):
    try:
        cursor = conn.cursor()

        # Create an index on is_premium_customer in Customer table
        cursor.execute('CREATE INDEX idx_customer_premium ON Customer(is_premium_customer)')

        conn.commit()
        print("Index on 'is_premium_customer' created successfully!")

    except psycopg2.Error as e:
        print("Error while creating index:", e)

    finally:
        cursor.close()

# Call the function to create the index
create_index_on_premium_customer(connect_cockroach())

# Indexing on navigation in Warehouse table:

def create_index_on_navigation(conn):
    try:
        cursor = conn.cursor()

        # Create an index on navigation in Warehouse table
        cursor.execute('CREATE INDEX idx_warehouse_navigation ON Warehouse(navigation)')

        conn.commit()
        print("Index on 'navigation' in Warehouse created successfully!")

    except psycopg2.Error as e:
        print("Error while creating index:", e)

    finally:
        cursor.close()

# Call the function to create the index
create_index_on_navigation(connect_cockroach())

def insert_products(conn, product_data):
    if conn:
        try:
            cursor = conn.cursor()

            # Batch insert for Product table
            cursor.executemany("INSERT INTO Product (name, weight) VALUES (%s, %s)", product_data)

            conn.commit()
            print("Batch insert for Products completed successfully!")

        except psycopg2.Error as e:
            print("Error during batch insert:", e)
            conn.rollback()

        finally:
            cursor.close()

product_data = [("Laptop", random.randint(1, 1000)) for _ in range(10)]

insert_products(connect_cockroach(), product_data)

def insert_warehouses(conn, warehouse_data):
    if conn:
        try:
            cursor = conn.cursor()

            # Batch insert for Warehouse table
            cursor.executemany("INSERT INTO Warehouse (location, capacity, navigation) VALUES (%s, %s, %s)", warehouse_data)

            conn.commit()
            print("Batch insert Warehouses completed successfully!")

        except psycopg2.Error as e:
            print("Error during batch insert:", e)
            conn.rollback()

        finally:
            cursor.close()

warehouse_data = [(Faker().city(), random.randint(100, 500), random.choice(['North', 'South', 'East', 'West'])) for _ in range(10)]

insert_warehouses(connect_cockroach(), warehouse_data)

