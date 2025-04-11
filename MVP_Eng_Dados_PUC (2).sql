-- Databricks notebook source
-- MAGIC %fs mkdirs /FileStore/tables/fashion_sales_data/

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/tables/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Criação e validação da camada Bronze**

-- COMMAND ----------

--Cria camada bronze. O "if not exists" foi necessário pois, como rodei o código algumas vezes, precisava apagar a tabela existente.
CREATE DATABASE IF NOT EXISTS fashion_bronze

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.customers;
CREATE TABLE fashion_bronze.customers (
  `Customer ID` STRING,
  `Name` STRING,
  `Email` STRING,
  `Telephone` STRING,
  `City` STRING,
  `Country` STRING,
  `Gender` STRING,
  `Date Of Birth` STRING,
  `Job Title` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/customers.csv'
OPTIONS (header true);

-- COMMAND ----------

--Teste de quantos valores nulos existem em cada coluna para verificação da quantidade dos dados
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Customer ID` IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN Name IS NULL THEN 1 ELSE 0 END) AS null_name,
  SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS null_email,
  SUM(CASE WHEN Telephone IS NULL THEN 1 ELSE 0 END) AS null_telephone,
  SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_city,
  SUM(CASE WHEN Country IS NULL THEN 1 ELSE 0 END) AS null_country,
  SUM(CASE WHEN Gender IS NULL THEN 1 ELSE 0 END) AS null_gender,
  SUM(CASE WHEN `Date Of Birth` IS NULL THEN 1 ELSE 0 END) AS null_dob,
  SUM(CASE WHEN `Job Title` IS NULL THEN 1 ELSE 0 END) AS null_job_title
FROM fashion_bronze.customers

-- COMMAND ----------

--Teste de validação de duplicidade na chave única (pk)
SELECT `Customer ID`, COUNT(*) 
FROM fashion_bronze.customers 
GROUP BY `Customer ID` 
HAVING COUNT(*) > 1;

-- COMMAND ----------

--Verificando se todos os e-mails estão no formato correto
SELECT Email, COUNT(*) as count 
FROM fashion_bronze.customers 
WHERE Email IS NOT NULL 
AND Email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
GROUP BY Email;

-- COMMAND ----------

--Verificando se datas de nascimento estão no formato correto
SELECT 
  SUM(CASE WHEN `Date Of Birth` > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_dates,
  SUM(CASE WHEN YEAR(`Date Of Birth`) < 1900 THEN 1 ELSE 0 END) AS too_old_dates,
  MIN(`Date Of Birth`) AS oldest_dob,
  MAX(`Date Of Birth`) AS most_recent_dob
FROM fashion_bronze.customers
WHERE `Date Of Birth` IS NOT NULL;

-- COMMAND ----------

--Verificando distribuição de clientes por pais
SELECT 
  Country, 
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fashion_bronze.customers), 2) AS percentage
FROM fashion_bronze.customers 
GROUP BY Country
ORDER BY customer_count DESC;

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.discounts;
CREATE TABLE fashion_bronze.discounts (
  `Start` STRING,
  `End` STRING,
  `Discont` STRING,
  `Description` STRING,
  `Category` STRING,
  `Sub Category` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/discounts.csv'
OPTIONS (header true);

-- COMMAND ----------

--Verificando registros nulos
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN Start IS NULL THEN 1 ELSE 0 END) AS null_start,
  SUM(CASE WHEN End IS NULL THEN 1 ELSE 0 END) AS null_end,
  SUM(CASE WHEN Discont IS NULL THEN 1 ELSE 0 END) AS null_discont,
  SUM(CASE WHEN Description IS NULL THEN 1 ELSE 0 END) AS null_description,
  SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_category,
  SUM(CASE WHEN `Sub Category` IS NULL THEN 1 ELSE 0 END) AS null_subcategory
FROM fashion_bronze.discounts;

-- COMMAND ----------

--Verificando se há algum desconto com datas inválidas, que acabam antes de começar
SELECT * 
FROM fashion_bronze.discounts 
WHERE End < Start;

-- COMMAND ----------

--Verificando se existem descontos de 0% ou maiores que 100%
SELECT Discont, COUNT(*) AS count
FROM fashion_bronze.discounts 
WHERE Discont < 0 OR Discont > 100
GROUP BY Discont
ORDER BY count DESC;

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.employees;
CREATE TABLE fashion_bronze.employees (
  `Employee ID` STRING,
  `Store ID` STRING,
  `Name` STRING,
  `Position` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/employees.csv'
OPTIONS (header true);

-- COMMAND ----------

--Verifica número de nulos por coluna
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Employee ID` IS NULL THEN 1 ELSE 0 END) AS null_employee_id,
  SUM(CASE WHEN `Store ID` IS NULL THEN 1 ELSE 0 END) AS null_store_id,
  SUM(CASE WHEN Name IS NULL THEN 1 ELSE 0 END) AS null_name,
  SUM(CASE WHEN Position IS NULL THEN 1 ELSE 0 END) AS null_position
FROM fashion_bronze.employees;

-- COMMAND ----------

--Verifica se existe id duplicado
SELECT `Employee ID`, COUNT(*) 
FROM fashion_bronze.employees 
GROUP BY `Employee ID` 
HAVING COUNT(*) > 1;

-- COMMAND ----------

--Verifica se todos os funcionários estão ligados a lojas que existem na tabela stores
SELECT 
  e.`Store ID`, 
  COUNT(*) AS employee_count,
  CASE 
    WHEN s.store_id IS NULL THEN 'NÃO EXISTE' 
    ELSE 'EXISTE' 
  END AS exists_in_stores
FROM fashion_bronze.employees e
LEFT JOIN (
  SELECT `Store ID` AS store_id
  FROM fashion_bronze.stores
) s ON e.`Store ID` = s.store_id
GROUP BY e.`Store ID`, exists_in_stores
ORDER BY employee_count DESC;

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.products;
CREATE TABLE fashion_bronze.products (
  `Product ID` STRING,
  `Category` STRING,
  `Sub Category` STRING,
  `Description PT` STRING,
  `Description DE` STRING,
  `Description FR` STRING,
  `Description ES` STRING,
  `Description EN` STRING,
  `Description ZH` STRING,
  `Color` STRING,
  `Sizes` STRING,
  `Production Cost` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/products.csv'
OPTIONS (header true);

-- COMMAND ----------

--Verifica valores nulos por coluna
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Product ID` IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_category,
  SUM(CASE WHEN `Sub Category` IS NULL THEN 1 ELSE 0 END) AS null_subcategory,
  SUM(CASE WHEN `Description PT` IS NULL THEN 1 ELSE 0 END) AS null_desc_pt,
  SUM(CASE WHEN `Description DE` IS NULL THEN 1 ELSE 0 END) AS null_desc_de,
  SUM(CASE WHEN `Description FR` IS NULL THEN 1 ELSE 0 END) AS null_desc_fr,
  SUM(CASE WHEN `Description ES` IS NULL THEN 1 ELSE 0 END) AS null_desc_es,
  SUM(CASE WHEN `Description EN` IS NULL THEN 1 ELSE 0 END) AS null_desc_en,
  SUM(CASE WHEN `Description ZH` IS NULL THEN 1 ELSE 0 END) AS null_desc_zh,
  SUM(CASE WHEN Color IS NULL THEN 1 ELSE 0 END) AS null_color,
  SUM(CASE WHEN Sizes IS NULL THEN 1 ELSE 0 END) AS null_sizes,
  SUM(CASE WHEN `Production Cost` IS NULL THEN 1 ELSE 0 END) AS null_production_cost
FROM fashion_bronze.products;

-- COMMAND ----------

--Verifica se existem ids duplicados
SELECT `Product ID`, COUNT(*) 
FROM fashion_bronze.products 
GROUP BY `Product ID` 
HAVING COUNT(*) > 1;

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.stores;
CREATE TABLE fashion_bronze.stores (
  `Store ID` STRING,
  `Country` STRING,
  `City` STRING,
  `Store Name` STRING,
  `Number of Employees` STRING,
  `ZIP Code` STRING,
  `Latitude` STRING,
  `Longitude` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/stores.csv'
OPTIONS (header true);

-- COMMAND ----------

--Verifica a existência de valores nulos por coluna
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Store ID` IS NULL THEN 1 ELSE 0 END) AS null_store_id,
  SUM(CASE WHEN Country IS NULL THEN 1 ELSE 0 END) AS null_country,
  SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_city,
  SUM(CASE WHEN `Store Name` IS NULL THEN 1 ELSE 0 END) AS null_store_name,
  SUM(CASE WHEN `Number of Employees` IS NULL THEN 1 ELSE 0 END) AS null_num_employees,
  SUM(CASE WHEN `ZIP Code` IS NULL THEN 1 ELSE 0 END) AS null_zipcode,
  SUM(CASE WHEN Latitude IS NULL THEN 1 ELSE 0 END) AS null_latitude,
  SUM(CASE WHEN Longitude IS NULL THEN 1 ELSE 0 END) AS null_longitude
FROM fashion_bronze.stores;

-- COMMAND ----------

--Verifica store ids duplicados
SELECT `Store ID`, COUNT(*) 
FROM fashion_bronze.stores 
GROUP BY `Store ID` 
HAVING COUNT(*) > 1;

-- COMMAND ----------

--Verifica coordenadas que não são válidas
SELECT 
  `Store ID`, 
  `Store Name`,
  Latitude, 
  Longitude
FROM fashion_bronze.stores
WHERE Latitude < -90 OR Latitude > 90 OR Longitude < -180 OR Longitude > 180
  OR Latitude IS NULL OR Longitude IS NULL;

-- COMMAND ----------

--Verifica se o número de funcionários é negativo em alguma linha
SELECT 
  `Store ID`, 
  `Store Name`,
  `Number of Employees`
FROM fashion_bronze.stores
WHERE `Number of Employees` <= 0 OR `Number of Employees` IS NULL;

-- COMMAND ----------

DROP TABLE IF EXISTS fashion_bronze.transactions;
CREATE TABLE fashion_bronze.transactions (
  `Invoice ID` STRING,
  `Line` STRING,
  `Customer ID` STRING,
  `Product ID` STRING,
  `Size` STRING,
  `Color` STRING,
  `Unit Price` STRING,
  `Quantity` STRING,
  `Date` STRING,
  `Discount` STRING,
  `Line Total` STRING,
  `Store ID` STRING,
  `Employee ID` STRING,
  `Currency` STRING,
  `Currency Symbol` STRING,
  `SKU` STRING,
  `Transaction Type` STRING,
  `Payment Method` STRING,
  `Invoice Total` STRING
)
USING CSV
LOCATION '/FileStore/tables/fashion_sales_data/transactions.csv'
OPTIONS (header true);

-- COMMAND ----------

--Verifica valores nulos em colunas essenciais
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Invoice ID` IS NULL THEN 1 ELSE 0 END) AS null_invoice_id,
  SUM(CASE WHEN Line IS NULL THEN 1 ELSE 0 END) AS null_line,
  SUM(CASE WHEN `Customer ID` IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN `Product ID` IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN `Unit Price` IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
  SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
  SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) AS null_date,
  SUM(CASE WHEN `Line Total` IS NULL THEN 1 ELSE 0 END) AS null_line_total,
  SUM(CASE WHEN `Store ID` IS NULL THEN 1 ELSE 0 END) AS null_store_id,
  SUM(CASE WHEN `Employee ID` IS NULL THEN 1 ELSE 0 END) AS null_employee_id
FROM fashion_bronze.transactions;

-- COMMAND ----------

--Verifica se algum valor numérico é negativo em colunas que devem ser positivas
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN `Unit Price` <= 0 THEN 1 ELSE 0 END) AS non_positive_unit_price,
  SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) AS non_positive_quantity,
  SUM(CASE WHEN Discount < 0 OR Discount > 1 THEN 1 ELSE 0 END) AS invalid_discount
FROM fashion_bronze.transactions;

-- COMMAND ----------

--Verifica transações com datas que não são válidas
SELECT 
  MIN(Date) AS earliest_date,
  MAX(Date) AS latest_date,
  COUNT(*) AS total_transactions,
  SUM(CASE WHEN Date > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_transactions,
  SUM(CASE WHEN YEAR(Date) < 2010 THEN 1 ELSE 0 END) AS very_old_transactions
FROM fashion_bronze.transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Criação da camada Silver**

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS fashion_silver

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_silver.customers AS ( 
SELECT 
  TRIM(`Customer ID`) AS customer_id,
  TRIM(`Name`) AS customer_name,
  TRIM(`Email`) AS email,
  TRIM(`Telephone`) AS telephone,
  TRIM(`City`) AS city,
  TRIM(`Country`) AS country,
  TRIM(`Gender`) AS gender,
  TO_DATE(`Date Of Birth`, 'yyyy-MM-dd') AS date_of_birth,
  TRIM(`Job Title`) AS job_title,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.customers);

-- COMMAND ----------

SELECT * 
FROM fashion_silver.customers 
LIMIT 1;


-- COMMAND ----------


CREATE OR REPLACE TABLE fashion_silver.discounts AS( 
SELECT 
  TO_DATE(`Start`, 'yyyy-MM-dd') AS start_date,
  TO_DATE(`End`, 'yyyy-MM-dd') AS end_date,
  CAST(REPLACE(`Discont`, '%', '') AS DECIMAL(5,2)) AS discount_percentage,
  TRIM(`Description`) AS description,
  TRIM(`Category`) AS category,
  TRIM(`Sub Category`) AS sub_category,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.discounts);

-- COMMAND ----------

SELECT * 
FROM fashion_silver.discounts
LIMIT 1

-- COMMAND ----------


CREATE OR REPLACE TABLE fashion_silver.employees AS (
SELECT 
  TRIM(`Employee ID`) AS employee_id,
  TRIM(`Store ID`) AS store_id,
  TRIM(`Name`) AS employee_name,
  TRIM(`Position`) AS position,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.employees);

-- COMMAND ----------

SELECT *
FROM fashion_silver.employees
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_silver.products AS ( 
SELECT 
  TRIM(`Product ID`) AS product_id,
  TRIM(`Category`) AS category,
  TRIM(`Sub Category`) AS sub_category,
  TRIM(`Description PT`) AS description_pt,
  TRIM(`Description DE`) AS description_de,
  TRIM(`Description FR`) AS description_fr,
  TRIM(`Description ES`) AS description_es,
  TRIM(`Description EN`) AS description_en,
  TRIM(`Description ZH`) AS description_zh,
  TRIM(`Color`) AS color,
  TRIM(`Sizes`) AS sizes,
  CAST(REPLACE(REPLACE(`Production Cost`, '$', ''), ',', '') AS DECIMAL(10,2)) AS production_cost,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.products);

-- COMMAND ----------

SELECT *
FROM fashion_silver.products
LIMIT 1

-- COMMAND ----------


CREATE OR REPLACE TABLE fashion_silver.stores AS (
SELECT 
  TRIM(`Store ID`) AS store_id,
  TRIM(`Country`) AS country,
  TRIM(`City`) AS city,
  TRIM(`Store Name`) AS store_name,
  CAST(`Number of Employees` AS INT) AS number_of_employees,
  TRIM(`ZIP Code`) AS zip_code,
  CAST(`Latitude` AS DOUBLE) AS latitude,
  CAST(`Longitude` AS DOUBLE) AS longitude,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.stores);

-- COMMAND ----------

SELECT * 
FROM fashion_silver.stores
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_silver.transactions AS (
SELECT 
  TRIM(`Invoice ID`) AS invoice_id,
  CAST(`Line` AS INT) AS line_number,
  TRIM(`Customer ID`) AS customer_id,
  TRIM(`Product ID`) AS product_id,
  TRIM(`Size`) AS size,
  TRIM(`Color`) AS color,
  CAST(REPLACE(REPLACE(`Unit Price`, '$', ''), ',', '') AS DECIMAL(10,2)) AS unit_price,
  CAST(`Quantity` AS INT) AS quantity,
  TO_TIMESTAMP(`Date`, 'yyyy-MM-dd HH:mm:ss') AS transaction_timestamp,
  CAST(REPLACE(`Discount`, '%', '') AS DECIMAL(5,2)) AS discount_percentage,
  CAST(REPLACE(REPLACE(`Line Total`, '$', ''), ',', '') AS DECIMAL(12,2)) AS line_total,
  TRIM(`Store ID`) AS store_id,
  TRIM(`Employee ID`) AS employee_id,
  TRIM(`Currency`) AS currency,
  TRIM(`Currency Symbol`) AS currency_symbol,
  TRIM(`SKU`) AS sku,
  TRIM(`Transaction Type`) AS transaction_type,
  TRIM(`Payment Method`) AS payment_method,
  CAST(REPLACE(REPLACE(`Invoice Total`, '$', ''), ',', '') AS DECIMAL(12,2)) AS invoice_total,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_bronze.transactions);

-- COMMAND ----------

SELECT *
FROM fashion_silver.transactions
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Criação da camada Gold**

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS fashion_gold

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.daily_sales_by_category AS
SELECT 
  t.transaction_timestamp,
  p.category,
  p.sub_category,
  COUNT(DISTINCT t.invoice_id) AS num_transactions,
  SUM(t.quantity) AS total_items_sold,
  SUM(t.line_total) AS total_revenue,
  SUM(t.line_total - (p.production_cost * t.quantity)) AS total_profit,
  AVG(t.discount_percentage) AS avg_discount,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.transactions t
JOIN fashion_silver.products p ON t.product_id = p.product_id
GROUP BY t.transaction_timestamp, p.category, p.sub_category;


-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.customer_profile AS
SELECT 
  c.customer_id,
  c.customer_name,
  c.gender,
  c.country,
  c.city,
  c.job_title,
  FLOOR(DATEDIFF(CURRENT_DATE(), c.date_of_birth)/365) AS age,
  COUNT(DISTINCT t.invoice_id) AS total_purchases,
  SUM(t.line_total) AS total_spent,
  AVG(t.line_total) AS avg_purchase_value,
  MAX(CAST(t.transaction_timestamp as date)) AS last_purchase_date,
  DATEDIFF(CURRENT_DATE(), MAX(CAST(t.transaction_timestamp as date))) AS days_since_last_purchase,
  ARRAY_AGG(DISTINCT p.category) AS preferred_categories,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.customers c
LEFT JOIN fashion_silver.transactions t ON c.customer_id = t.customer_id
LEFT JOIN fashion_silver.products p ON t.product_id = p.product_id
GROUP BY c.customer_id, c.customer_name, c.gender, c.country, c.city, c.job_title, c.date_of_birth;

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.store_performance AS
SELECT 
  s.store_id,
  s.store_name,
  s.country,
  s.city,
  s.number_of_employees,
  COUNT(DISTINCT t.invoice_id) AS total_transactions,
  SUM(t.line_total) AS total_revenue,
  SUM(t.line_total) / s.number_of_employees AS revenue_per_employee,
  COUNT(DISTINCT t.customer_id) AS unique_customers,
  AVG(t.discount_percentage) AS avg_discount,
  SUM(t.line_total - (p.production_cost * t.quantity)) AS total_profit,
  (SUM(t.line_total - (p.production_cost * t.quantity)) / SUM(t.line_total)) * 100 AS profit_margin,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.stores s
LEFT JOIN fashion_silver.transactions t ON s.store_id = t.store_id
LEFT JOIN fashion_silver.products p ON t.product_id = p.product_id
GROUP BY s.store_id, s.store_name, s.country, s.city, s.number_of_employees;

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.employee_performance AS
SELECT 
  e.employee_id,
  e.employee_name,
  e.position,
  e.store_id,
  s.store_name,
  COUNT(DISTINCT t.invoice_id) AS total_sales,
  SUM(t.line_total) AS total_revenue,
  AVG(t.line_total) AS avg_sale_value,
  SUM(t.line_total - (p.production_cost * t.quantity)) AS total_profit_generated,
  COUNT(DISTINCT t.customer_id) AS unique_customers,
  ARRAY_AGG(DISTINCT p.category) AS top_categories_sold,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.employees e
JOIN fashion_silver.stores s ON e.store_id = s.store_id
LEFT JOIN fashion_silver.transactions t ON e.employee_id = t.employee_id
LEFT JOIN fashion_silver.products p ON t.product_id = p.product_id
GROUP BY e.employee_id, e.employee_name, e.position, e.store_id, s.store_name;

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.product_performance AS
SELECT 
  p.product_id,
  p.category,
  p.sub_category,
  p.description_en,
  p.color,
  p.sizes,
  p.production_cost,
  COUNT(t.line_number) AS times_sold,
  SUM(t.quantity) AS total_quantity_sold,
  AVG(t.unit_price) AS avg_selling_price,
  SUM(t.line_total) AS total_revenue,
  SUM(t.line_total - (p.production_cost * t.quantity)) AS total_profit,
  (SUM(t.line_total - (p.production_cost * t.quantity)) / SUM(t.line_total)) * 100 AS profit_margin,
  AVG(t.discount_percentage) AS avg_discount_applied,
  COUNT(DISTINCT t.customer_id) AS unique_customers,
  ARRAY_AGG(DISTINCT t.size) AS sold_sizes,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.products p
LEFT JOIN fashion_silver.transactions t ON p.product_id = t.product_id
GROUP BY p.product_id, p.category, p.sub_category, p.description_en, p.color, p.sizes, p.production_cost;

-- COMMAND ----------

CREATE OR REPLACE TABLE fashion_gold.discount_effectiveness AS
SELECT 
  d.start_date,
  d.end_date,
  d.description AS campaign_description,
  d.category,
  d.sub_category,
  d.discount_percentage AS offered_discount,
  COUNT(t.line_number) AS total_sales,
  SUM(t.quantity) AS total_items_sold,
  SUM(t.line_total) AS revenue_during_campaign,
  AVG(t.discount_percentage) AS actual_avg_discount,
  SUM(t.line_total - (p.production_cost * t.quantity)) AS total_profit,
  CURRENT_TIMESTAMP() AS processed_at
FROM fashion_silver.discounts d
JOIN fashion_silver.transactions t ON 
  t.transaction_timestamp BETWEEN d.start_date AND d.end_date AND
  t.discount_percentage > 0
JOIN fashion_silver.products p ON 
  t.product_id = p.product_id AND
  p.category = d.category AND
  p.sub_category = d.sub_category
GROUP BY d.start_date, d.end_date, d.description, d.category, d.sub_category, d.discount_percentage;

-- COMMAND ----------

--Consultando os produtos com melhor desempenho por receita total
SELECT 
  product_id, 
  description_en, 
  category, 
  sub_category, 
  total_revenue, 
  total_quantity_sold, 
  total_profit, 
  profit_margin
FROM fashion_gold.product_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

--Consultando os produtos com melhor desempenho por quantidade vendida
SELECT 
  product_id, 
  description_en, 
  category, 
  sub_category, 
  total_quantity_sold, 
  total_revenue, 
  total_profit, 
  profit_margin
FROM fashion_gold.product_performance
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- COMMAND ----------

--Consultando os produtos com melhor desempenho por lucratividade
SELECT 
  product_id, 
  description_en, 
  category, 
  sub_category, 
  total_profit, 
  profit_margin, 
  total_revenue, 
  total_quantity_sold
FROM fashion_gold.product_performance
ORDER BY total_profit DESC
LIMIT 10;

-- COMMAND ----------

--Consultando tendências de vendas diárias
SELECT 
  transaction_timestamp,
  SUM(num_transactions) AS daily_transactions,
  SUM(total_items_sold) AS daily_items_sold,
  SUM(total_revenue) AS daily_revenue,
  SUM(total_profit) AS daily_profit
FROM fashion_gold.daily_sales_by_category
GROUP BY transaction_timestamp
ORDER BY transaction_timestamp;

-- COMMAND ----------

--Consultando tendências de vendas por mês
SELECT 
  YEAR(transaction_timestamp) AS year,
  MONTH(transaction_timestamp) AS month,
  SUM(num_transactions) AS monthly_transactions,
  SUM(total_items_sold) AS monthly_items_sold,
  SUM(total_revenue) AS monthly_revenue,
  SUM(total_profit) AS monthly_profit
FROM fashion_gold.daily_sales_by_category
GROUP BY YEAR(transaction_timestamp), MONTH(transaction_timestamp)
ORDER BY year, month;

-- COMMAND ----------

SELECT 
  YEAR(transaction_timestamp) AS year,
  MONTH(transaction_timestamp) AS month,
  category,
  SUM(num_transactions) AS monthly_transactions,
  SUM(total_items_sold) AS monthly_items_sold,
  SUM(total_revenue) AS monthly_revenue
FROM fashion_gold.daily_sales_by_category
GROUP BY year, month, category
ORDER BY year, month, monthly_revenue DESC;

-- COMMAND ----------

--Consultando campanhas por receita gerada
SELECT 
  campaign_description,
  category,
  sub_category,
  start_date,
  end_date,
  DATEDIFF(end_date, start_date) AS campaign_duration_days,
  offered_discount,
  actual_avg_discount,
  total_sales,
  total_items_sold,
  revenue_during_campaign,
  revenue_during_campaign / DATEDIFF(end_date, start_date) AS daily_revenue,
  total_profit,
  (total_profit / revenue_during_campaign) * 100 AS profit_margin
FROM fashion_gold.discount_effectiveness
ORDER BY revenue_during_campaign DESC;

-- COMMAND ----------

--Consultando campanhas por ROI (retorno sobre investimento em desconto)
SELECT 
  campaign_description,
  category,
  sub_category,
  offered_discount,
  actual_avg_discount,
  total_sales,
  revenue_during_campaign,
  total_profit,
  total_profit / (revenue_during_campaign * actual_avg_discount / 100) AS estimated_roi
FROM fashion_gold.discount_effectiveness
WHERE actual_avg_discount > 0
ORDER BY estimated_roi DESC;

-- COMMAND ----------

--Consultando desempenho de lojas por receita total
SELECT 
  store_id,
  store_name,
  country,
  city,
  total_transactions,
  total_revenue,
  total_profit,
  profit_margin,
  revenue_per_employee,
  unique_customers
FROM fashion_gold.store_performance
ORDER BY total_revenue DESC;

-- COMMAND ----------

--Consultando lojas por eficiência (receita por funcionário)
SELECT 
  store_id,
  store_name,
  country,
  city,
  number_of_employees,
  total_revenue,
  revenue_per_employee,
  total_profit,
  profit_margin
FROM fashion_gold.store_performance
ORDER BY revenue_per_employee DESC;

-- COMMAND ----------

--Consultando desempenho de lojas por receita total
SELECT 
  store_id,
  store_name,
  country,
  city,
  total_transactions,
  total_revenue,
  total_profit,
  profit_margin,
  revenue_per_employee,
  unique_customers
FROM fashion_gold.store_performance
ORDER BY total_revenue DESC;

-- COMMAND ----------

--Consultando desempenho por país
SELECT 
  country,
  COUNT(*) AS num_stores,
  SUM(total_transactions) AS total_transactions,
  SUM(total_revenue) AS total_revenue,
  SUM(total_profit) AS total_profit,
  SUM(total_profit) / SUM(total_revenue) * 100 AS overall_profit_margin,
  SUM(unique_customers) AS total_unique_customers,
  SUM(total_revenue) / COUNT(*) AS avg_revenue_per_store
FROM fashion_gold.store_performance
GROUP BY country
ORDER BY total_revenue DESC;

-- COMMAND ----------

--Top10 cidades mais lucrativas
SELECT 
  country,
  city,
  COUNT(*) AS num_stores,
  SUM(total_transactions) AS total_transactions,
  SUM(total_revenue) AS total_revenue,
  SUM(total_profit) AS total_profit,
  SUM(total_profit) / SUM(total_revenue) * 100 AS overall_profit_margin
FROM fashion_gold.store_performance
GROUP BY country, city
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

--Consultando categorias por margem de lucro
SELECT 
  category,
  sub_category,
  COUNT(*) AS num_products,
  SUM(total_quantity_sold) AS total_quantity_sold,
  SUM(total_revenue) AS total_revenue,
  SUM(total_profit) AS total_profit,
  SUM(total_profit) / SUM(total_revenue) * 100 AS category_profit_margin
FROM fashion_gold.product_performance
GROUP BY category, sub_category
ORDER BY category_profit_margin DESC;


-- COMMAND ----------

--Top20 produtos mais rentáveis (maior margem de lucro)
SELECT 
  product_id,
  description_en,
  category,
  sub_category,
  production_cost,
  avg_selling_price,
  total_quantity_sold,
  total_revenue,
  total_profit,
  profit_margin
FROM fashion_gold.product_performance
WHERE total_quantity_sold > 10 
ORDER BY profit_margin DESC
LIMIT 20;

-- COMMAND ----------

--Consultando sazonalidade de vendas por mês
SELECT 
  YEAR(transaction_timestamp) AS year,
  MONTH(transaction_timestamp) AS month,
  category,
  sub_category,
  SUM(total_items_sold) AS monthly_quantity_sold,
  SUM(total_revenue) AS monthly_revenue
FROM fashion_gold.daily_sales_by_category
GROUP BY year, month, category, sub_category
ORDER BY category, sub_category, year, month;

-- COMMAND ----------

--Pico de meses por categoria de produto
WITH monthly_sales AS (
  SELECT 
    MONTH(transaction_timestamp) AS month,
    category,
    SUM(total_items_sold) AS monthly_quantity_sold
  FROM fashion_gold.daily_sales_by_category
  GROUP BY month, category
),
category_totals AS (
  SELECT 
    category,
    SUM(monthly_quantity_sold) AS total_quantity_sold
  FROM monthly_sales
  GROUP BY category
)
SELECT 
  ms.month,
  ms.category,
  ms.monthly_quantity_sold,
  ms.monthly_quantity_sold / ct.total_quantity_sold * 100 AS percentage_of_annual_sales,
  RANK() OVER (PARTITION BY ms.category ORDER BY ms.monthly_quantity_sold DESC) AS month_rank
FROM monthly_sales ms
JOIN category_totals ct ON ms.category = ct.category
ORDER BY ms.category, month_rank;

-- COMMAND ----------

--Clientes por frequência e valor
SELECT 
  CASE 
    WHEN total_purchases >= 5 AND total_spent >= 1000 THEN 'VIP'
    WHEN total_purchases >= 3 AND total_spent >= 500 THEN 'Regular'
    WHEN days_since_last_purchase <= 90 THEN 'Recente'
    ELSE 'Ocasional'
  END AS customer_segment,
  COUNT(*) AS segment_size,
  AVG(total_purchases) AS avg_purchases,
  AVG(total_spent) AS avg_spend,
  AVG(avg_purchase_value) AS avg_order_value,
  SUM(total_spent) AS segment_total_spend,
  SUM(total_spent) / SUM(total_purchases) AS segment_avg_order_value
FROM fashion_gold.customer_profile
GROUP BY customer_segment
ORDER BY avg_spend DESC;

-- COMMAND ----------

--Análise de clientes por gênero
SELECT 
  gender,
  COUNT(*) AS customer_count,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fashion_gold.customer_profile) AS percentage,
  AVG(age) AS avg_age,
  AVG(total_purchases) AS avg_purchases,
  AVG(total_spent) AS avg_total_spent,
  SUM(total_spent) AS total_revenue
FROM fashion_gold.customer_profile
GROUP BY gender
ORDER BY customer_count DESC;

-- COMMAND ----------


--Análise de clientes por faixa etária
SELECT 
  CASE 
    WHEN age < 18 THEN 'Under 18'
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 44 THEN '35-44'
    WHEN age BETWEEN 45 AND 54 THEN '45-54'
    WHEN age BETWEEN 55 AND 64 THEN '55-64'
    WHEN age >= 65 THEN '65+'
    ELSE 'Unknown'
  END AS age_group,
  COUNT(*) AS customer_count,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fashion_gold.customer_profile) AS percentage,
  AVG(total_purchases) AS avg_purchases,
  AVG(total_spent) AS avg_total_spent
  FROM fashion_gold.customer_profile
  GROUP BY age_group