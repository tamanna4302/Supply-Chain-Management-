#!/usr/bin/env python
# coding: utf-8

# In[31]:


from pymongo import MongoClient
from datetime import datetime

# Establish connection to MongoDB Docker container
client = MongoClient('localhost', 27017)  # Assuming MongoDB is running on localhost:27017
db = client['supply_chain']  # Create or connect to the supply_chain_db database

# Define schemas for different entities
product_schema = {"product_id": "string","name": "string"}

warehouse_schema = {
    "warehouse_id": "string",
    "location": "string",
    "capacity": "int"
}

available_product_schema = {
    "warehouse_id": "string",
    "product_id": "string",
    "quantity": "int"
}

customer_schema = {
    "customer_id": "string",
    "name": "string",
    "ispremium": "boolean",
    "address": "string"
}

order_schema = {
    "order_id": "string",
    "product_id": "string",
    "date": "int",
    "payment": "float",
    "status":"string"
}



# Create collections based on schemas
collection_product = db['products']
collection_warehouse = db['warehouses']
collection_available_product = db['available_products']
collection_customer = db['customers']
collection_order = db['orders']



# Function to insert multiple product documents
def insert_multiple_products(products_data):
    # Insert valid products into the collection
    if products_data:
        result = collection_product.insert_many(products_data)
        print(f"{len(result.inserted_ids)} products inserted.")

        # Retrieve the inserted documents and print them
        inserted_ids = result.inserted_ids
        inserted_documents = collection_product.find({"_id": {"$in": inserted_ids}})
        print("Inserted Items:")
        for doc in inserted_documents:
            print(doc)
    else:
        print("No valid products to insert.")

# Sample data for insertion (five products)
sample_products_data = [
    {
        "product_id": "P001",
        "name": "Laptop",

    },
    {
        "product_id": "P002",
        "name": "Desktop",
        
    },
    {
        "product_id": "P003",
        "name": "Blender",
       
    },
    {
        "product_id": "P004",
        "name": "Oven",
        
    },
    {
        "product_id": "P005",
        "name": "Fridge",
        
    }
]


# In[32]:


insert_multiple_products(sample_products_data)


# In[33]:


def insert_multiple_warehouses(warehouses_data):
    # Insert valid warehouses into the collection
    if warehouses_data:
        result = collection_warehouse.insert_many(warehouses_data)
        print(f"{len(result.inserted_ids)} warehouses inserted.")

        # Retrieve the inserted documents and print them
        inserted_ids = result.inserted_ids
        inserted_documents = collection_warehouse.find({"_id": {"$in": inserted_ids}})
        print("Inserted Warehouse Items:")
        for doc in inserted_documents:
            print(doc)
    else:
        print("No valid warehouses to insert.")

# Sample data for insertion (five warehouses)
sample_warehouses_data = [
    {
        "warehouse_id": "W001",
        "location": "Tempe",
        "capacity": 1000
    },
    {
        "warehouse_id": "W002",
        "location": "Phoenix",
        "capacity": 1500
    },
    {
        "warehouse_id": "W003",
        "location": "Scottsdale",
        "capacity": 1200
    },
    {
        "warehouse_id": "W004",
        "location": "Chandler",
        "capacity": 1800
    },
    {
        "warehouse_id": "W005",
        "location": "Mesa",
        "capacity": 2000
    }
]


# In[34]:


insert_multiple_warehouses(sample_warehouses_data)


# In[35]:


def insert_multiple_available_products(available_products_data):
    # Insert valid available products into the collection
    if available_products_data:
        result = collection_available_product.insert_many(available_products_data)
        print(f"{len(result.inserted_ids)} available products inserted.")

        # Retrieve the inserted documents and print them
        inserted_ids = result.inserted_ids
        inserted_documents = collection_available_product.find({"_id": {"$in": inserted_ids}})
        print("Inserted Available Product Items:")
        for doc in inserted_documents:
            print(doc)
    else:
        print("No valid available products to insert.")

# Sample data for insertion (five available products)
sample_available_products_data = [
    {
        "warehouse_id": "W001",
        "product_id": "P001",
        "quantity": 100
    },
    {
        "warehouse_id": "W002",
        "product_id": "P002",
        "quantity": 200
    },
    {
        "warehouse_id": "W001",
        "product_id": "P003",
        "quantity": 150
    },
    {
        "warehouse_id": "W003",
        "product_id": "P002",
        "quantity": 300
    },
    {
        "warehouse_id": "W002",
        "product_id": "P001",
        "quantity": 120
    }
]


# In[36]:


insert_multiple_available_products(sample_available_products_data)


# In[37]:


def insert_multiple_customers(customers_data):
    # Insert valid customers into the collection
    if customers_data:
        result = collection_customer.insert_many(customers_data)
        print(f"{len(result.inserted_ids)} customers inserted.")

        # Retrieve the inserted documents and print them
        inserted_ids = result.inserted_ids
        inserted_documents = collection_customer.find({"_id": {"$in": inserted_ids}})
        print("Inserted Customer Items:")
        for doc in inserted_documents:
            print(doc)
    else:
        print("No valid customers to insert.")

# Sample data for insertion (five customers)
sample_customers_data = [
    {
        "customer_id": "C001",
        "name": "Riya",
        "ispremium": True,
        "address": "717 Farmer Ave"
    },
    {
        "customer_id": "C002",
        "name": "Siya",
        "ispremium": False,
        "address": "707 W Broadway Rd"
    },
    {
        "customer_id": "C003",
        "name": "Sara",
        "ispremium": True,
        "address": "101 College Ave"
    },
    {
        "customer_id": "C004",
        "name": "Jenny",
        "ispremium": True,
        "address": "404 Apache Blvd"
    },
    {
        "customer_id": "C005",
        "name": "Noel",
        "ispremium": False,
        "address": "505 Hardy Dr"
    }
]


# In[38]:


insert_multiple_customers(sample_customers_data)


# In[39]:


def insert_multiple_orders(orders_data):
    # Insert valid orders into the collection
    if orders_data:
        result = collection_order.insert_many(orders_data)
        print(f"{len(result.inserted_ids)} orders inserted.")

        # Retrieve the inserted documents and print them
        inserted_ids = result.inserted_ids
        inserted_documents = collection_order.find({"_id": {"$in": inserted_ids}})
        print("Inserted Order Items:")
        for doc in inserted_documents:
            print(doc)
    else:
        print("No valid orders to insert.")

# Sample data for insertion (five orders)
sample_orders_data = [
    {
        "order_id": "O001",
        "product_id": "P001",
        "date": 20231125,
        "payment": 150.99,
        "status": "Completed"
    },
    {
        "order_id": "O002",
        "product_id": "P002",
        "date": 20231126,
        "payment": 200.5,
        "status": "Pending"
    },
    {
        "order_id": "O003",
        "product_id": "P001",
        "date": 20231127,
        "payment": 75.25,
        "status": "Completed"
    },
    {
        "order_id": "O004",
        "product_id": "P003",
        "date": 20231128,
        "payment": 300.0,
        "status": "Shipped"
    },
    {
        "order_id": "O005",
        "product_id": "P002",
        "date": 20231129,
        "payment": 180.75,
        "status": "Processing"
    }
]


# In[40]:


insert_multiple_orders(sample_orders_data)


# In[41]:


# Function to print documents for a specific collection
def print_collection_data(collection_name):
    collection = db[collection_name]
    documents = collection.find()  # Retrieve all documents in the collection

    print(f"Documents in collection '{collection_name}':")
    for doc in documents:
        print(doc)
        print("------------------------------------")


# In[42]:


collections_to_check = ['products', 'warehouses', 'available_products', 'customers', 'orders']
for collection_name in collections_to_check:
        print_collection_data(collection_name)


# In[43]:


def update_and_print_product(product_id, updated_data):
    collection_product = db['products']
    
    # Define the filter to find the specific product to update
    filter_query = {"product_id": product_id}
    
    # Define the update operation - in this case, updating multiple fields
    update_query = {"$set": updated_data}
    
    # Update a single record based on the filter
    result = collection_product.update_one(filter_query, update_query)
    
    # Check if the update was successful
    if result.modified_count > 0:
        print("Product record updated successfully.")
        # Retrieve and print the updated record
        updated_record = collection_product.find_one(filter_query)
        print("Updated Product Record:")
        print(updated_record)
    else:
        print("No matching product found or no changes made.")


# In[44]:


product_id_to_update = "P001"  # Product ID of the record to update
updated_product_data = {"name":"Mixer Grinder"}
update_product(product_id_to_update, updated_product_data)


# In[45]:


def delete_order(order_id):
    collection_orders = db['orders']
    
    # Define the filter to find the specific order to delete
    filter_query = {"order_id": order_id}
    
    # Delete a single record based on the filter
    result = collection_orders.delete_one(filter_query)
    
    # Check if the deletion was successful
    if result.deleted_count > 0:
        print("Order record deleted successfully.")
    else:
        print("No matching order found for deletion.")


# In[46]:


order_id_to_delete = "O001"  # Order ID of the record to delete
delete_order(order_id_to_delete)


# In[47]:


def retrieve_all_products():
    collection_products = db['products']
    all_products = collection_products.find()
    return list(all_products)

products = retrieve_all_products()
for product in products:
        print(product)


# In[48]:


def retrieve_all_warehouses():
    collection_warehouses = db['warehouses']
    all_warehouses = collection_warehouses.find()
    return list(all_warehouses)

warehouses = retrieve_all_warehouses()
for warehouse in warehouses:
        print(warehouse)

