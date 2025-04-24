CREATE EXTERNAL TABLE transactions_v2 (
    transaction_id INT,
    user_id INT,
    amount DOUBLE,
    currency STRING,
    transaction_date TIMESTAMP,
    is_fraud INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA INPATH 's3a://storages/transactions_v2.csv' INTO TABLE transactions_v2;

CREATE EXTERNAL TABLE logs_v2 (
    log_id INT,
    transaction_id INT,
    category STRING,
    comment STRING,
    log_timestamp TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;
LOAD DATA INPATH 's3a://storages/logs_v2.txt' INTO TABLE logs_v2;

-- 1. “Хорошие” валюты: USD | EUR | RUB – суммарные amount 
SELECT
    currency,
    SUM(amount) AS total_amount,
    ROUND(AVG(amount),2) AS avg_amount,
    COUNT(*) AS operations
FROM transactions_v2
WHERE currency IN ('USD','EUR','RUB')
GROUP BY currency
ORDER BY total_amount DESC;

-- 2. Fraud vs Non-Fraud: счётчик, суммы, средний чек 
SELECT
    CASE WHEN is_fraud = 1 THEN 'Fraud' ELSE 'Normal' END AS txn_type,
    COUNT(*)                          AS txn_cnt,
    SUM(amount)                       AS total_amount,
    ROUND(AVG(amount),2)              AS avg_amount
FROM transactions_v2
GROUP BY is_fraud;

-- 3. Ежедневная динамика: количество, сумма, средний amount  
SELECT
    DATE(transaction_date)                AS txn_day,
    COUNT(*)                              AS txn_cnt,
    SUM(amount)                           AS total_amount,
    ROUND(AVG(amount),2)                  AS avg_amount
FROM transactions_v2
GROUP BY DATE(transaction_date)
ORDER BY txn_day;