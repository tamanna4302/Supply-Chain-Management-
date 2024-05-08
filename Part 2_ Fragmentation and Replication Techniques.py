#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def horizontal_fragmentation_premium_customers():
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Premium Customers Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS PremiumCustomers AS SELECT * FROM Customer WHERE is_premium_customer = True')

            # Regular Customers Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS RegularCustomers AS SELECT * FROM Customer WHERE is_premium_customer = False')

            conn.commit()
            print("Horizontal fragmentation for Premium and Regular Customers completed successfully!")

        except psycopg2.Error as e:
            print("Error while performing horizontal fragmentation:", e)

        finally:
            cursor.close()
            conn.close()
horizontal_fragmentation_premium_customers()
print_table_data("PremiumCustomers")
print_table_data("RegularCustomers")


def horizontal_fragmentation_warehouse_regions():
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Fragmentation 1: Products in North Region
            cursor.execute('CREATE TABLE IF NOT EXISTS Warehouse_North AS SELECT * FROM Warehouse WHERE navigation = \'North\'')

            # Fragmentation 2: Products in South Region
            cursor.execute('CREATE TABLE IF NOT EXISTS Warehouse_South AS SELECT * FROM Warehouse WHERE navigation = \'South\'')

            # Fragmentation 3: Products in West Region
            cursor.execute('CREATE TABLE IF NOT EXISTS Warehouse_West AS SELECT * FROM Warehouse WHERE navigation = \'West\'')

            # Fragmentation 4: Products in East Region
            cursor.execute('CREATE TABLE IF NOT EXISTS Warehouse_East AS SELECT * FROM Warehouse WHERE navigation = \'East\'')

            conn.commit()
            print("Horizontal fragmentation for Warehouse based on regions completed successfully!")

        except psycopg2.Error as e:
            print("Error while performing horizontal fragmentation for Warehouse:", e)

        finally:
            cursor.close()
            conn.close()
horizontal_fragmentation_warehouse_regions()

def vertical_fragmentation_product():
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Essential Product Information Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS EssentialProductInfo AS SELECT product_id, name FROM Product')

            # Additional Product Details Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS AdditionalProductDetails AS SELECT product_id, weight FROM Product')

            conn.commit()
            print("Vertical Fragmentation Product created successfully!")

        except psycopg2.Error as e:
            print("Error while creating fragmented tables:", e)

        finally:
            cursor.close()
            conn.close()
vertical_fragmentation_product()


def vertical_fragmentation_order_details():
    conn = connect_cockroach()
    if conn:
        try:
            cursor = conn.cursor()

            # Basic Order Information Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS BasicOrderInfo AS SELECT order_id, customer_id, date, status FROM OrderDetails')

            # Payment Details Subset
            cursor.execute('CREATE TABLE IF NOT EXISTS PaymentDetails AS SELECT order_id, payment FROM OrderDetails')

            conn.commit()
            print("Vertical Fragmentation Order Details created successfully!")

        except psycopg2.Error as e:
            print("Error while creating fragmented tables:", e)

        finally:
            cursor.close()
            conn.close()
vertical_fragmentation_order_details()

# conn = connect_cockroach()
# cursor = conn.cursor()
# cursor.execute('ALTER TABLE customers CONFIGURE ZONE USING num_replicas = 3, constraints = '[+navigation=east, +navigation=west1, +navigation=west]')

