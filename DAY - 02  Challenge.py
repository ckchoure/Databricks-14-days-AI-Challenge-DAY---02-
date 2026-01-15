# Databricks notebook source
# DBTITLE 1,Untitled
# import data 
data = spark.read.csv("/Volumes/workspace/default/data/product_sales_solution - orders.csv", header= True, inferSchema=True)

# print data headers
data.printSchema()

# COMMAND ----------

# select columns
data.select("customer_id", "qty", "total_price").show(5)

# COMMAND ----------

# group by customer_name and count 
data.groupBy("customer_name").count().show()

# COMMAND ----------

# group by product_name and arrange in descending order by count of product_name
Top_cust = data.groupBy("product_name").count().orderBy("count", ascending=False)
Top_cust.show()

# COMMAND ----------

# group by product_name and arrange in descending order by count of product_name and selecting top 5
Top_customer = data.groupBy("product_name").count().orderBy("count", ascending=False).limit(5)
Top_customer.show()

# COMMAND ----------

# DBTITLE 1,Cell 6
# write data to csv
Top_customer.write.mode("overwrite").csv("/Volumes/workspace/default/data/Top_customer")