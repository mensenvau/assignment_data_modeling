# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the Customers table to store customer information
# MAGIC create table if not exists Customers (
# MAGIC   customer_id int, --  primary key
# MAGIC   name string not null,
# MAGIC   email string not null,
# MAGIC   phone string,
# MAGIC   address string
# MAGIC );
# MAGIC
# MAGIC -- Create the Products table to store product information
# MAGIC create table if not exists Products (
# MAGIC   product_id int , -- primary key
# MAGIC   name string not null,
# MAGIC   description string,
# MAGIC   last_price decimal(10, 2) not null
# MAGIC );
# MAGIC
# MAGIC -- Create the Orders table to store order details
# MAGIC create table if not exists Orders (
# MAGIC   order_id int , -- primary key
# MAGIC   customer_id int not null,
# MAGIC   order_date timestamp,
# MAGIC   status string,
# MAGIC   total_amount decimal(10, 2)
# MAGIC   -- ,foreign key (customer_id) references Customers(customer_id)
# MAGIC );
# MAGIC
# MAGIC -- Create the OrderItems table to store details of individual items in an order
# MAGIC create table if not exists OrderItems (
# MAGIC   item_id int , -- primary key
# MAGIC   order_id int not null,
# MAGIC   product_id int not null,
# MAGIC   quantity int not null,
# MAGIC   amount decimal(10, 2),
# MAGIC   status string not null 
# MAGIC   -- ,foreign key (order_id) references Orders(order_id),
# MAGIC   -- foreign key (product_id) references Products(product_id)
# MAGIC );

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/data", True) # for clear

# COMMAND ----------

# Schema for the Customers table
customers_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),  # Primary key
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True)
])

# Schema for the Products table
products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),  # Primary key
    StructField("name", StringType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("last_price", DecimalType(10, 2), nullable=False)
])

# Schema for the Orders table
orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),  # Primary key
    StructField("customer_id", IntegerType(), nullable=False),  # Foreign key to Customers
    StructField("order_date", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("total_amount", DecimalType(10, 2), nullable=True)
])

# Schema for the OrderItems table
order_items_schema = StructType([
    StructField("item_id", IntegerType(), nullable=False),  # Primary key
    StructField("order_id", IntegerType(), nullable=False),  # Foreign key to Orders
    StructField("product_id", IntegerType(), nullable=False),  # Foreign key to Products
    StructField("quantity", IntegerType(), nullable=False),
    StructField("amount", DecimalType(10, 2), nullable=True),
    StructField("status", StringType(), nullable=False)
])


# COMMAND ----------

# Read CSV files with specified schema and load into DataFrames
customers = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/data/customers_data.csv", schema=customers_schema)
products = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/data/products_data.csv", schema=products_schema)
orders = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/data/orders_data.csv", schema=orders_schema)
order_items = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/data/order_items_data.csv", schema=order_items_schema)

# Write DataFrames to Delta tables, overwriting existing data
customers.write.format("delta").mode("overwrite").saveAsTable("Customers")
products.write.format("delta").mode("overwrite").saveAsTable("Products")
orders.write.format("delta").mode("overwrite").saveAsTable("Orders")
order_items.write.format("delta").mode("overwrite").saveAsTable("OrderItems")
