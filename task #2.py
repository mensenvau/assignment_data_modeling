# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- select * from Customers;
# MAGIC  select * from Products;
# MAGIC -- select * from Orders;
# MAGIC -- select * from OrderItems;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Task 2: Provide queries to find the most sold product in the last month.
# MAGIC
# MAGIC with cte as (
# MAGIC   select
# MAGIC       p.product_id, 
# MAGIC       p.name,
# MAGIC       sum(oi.quantity) as count,
# MAGIC       dense_rank() over(order by sum(oi.quantity) desc) as rn
# MAGIC   from OrderItems oi
# MAGIC   inner join Orders o on o.order_id = oi.order_id
# MAGIC   inner join Products p on p.product_id = oi.product_id
# MAGIC   where year(o.order_date ) = year(current_timestamp) and month(o.order_date ) = month(current_timestamp)
# MAGIC   group by p.product_id, p.name
# MAGIC )
# MAGIC
# MAGIC select * from cte where rn = 1
