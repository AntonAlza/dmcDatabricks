# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="g5-key-vault-Antony")

# COMMAND ----------

password = dbutils.secrets.get(scope= "g5-key-vault-Antony", key="contraseniaPostgre")

# COMMAND ----------

print(password)

# COMMAND ----------

driver = "org.postgresql.Driver"
database_host = "espdatabricks5.postgres.database.azure.com"
database_port = "5432"
database_name = "Northwind"
table = "orders"
user = "grupo5_Antony"
password = password

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"


# COMMAND ----------

sql_categories = (spark.read.format("jdbc").option("driver",driver).option("url",url).option("dbtable","categories").option("user",user).option("password",password).load())

# COMMAND ----------

sql_categories.show()
