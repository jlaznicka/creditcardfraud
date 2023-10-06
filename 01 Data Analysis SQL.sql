-- Select from the main table

SELECT *
FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD;

----------------------------------------------------------------------------------------------------------------------------------

-- Total count of rows

SELECT COUNT(*) AS total_rows FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD;

----------------------------------------------------------------------------------------------------------------------------------

-- Summary statistics for TX_AMOUNT

SELECT
    MIN(TX_AMOUNT) AS min_amount,
    MAX(TX_AMOUNT) AS max_amount,
    AVG(TX_AMOUNT) AS avg_amount,
    SUM(TX_AMOUNT) AS total_amount
FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD;

----------------------------------------------------------------------------------------------------------------------------------

-- Distribution of transaction ammounts between fraud scenarious

SELECT 
    TX_FRAUD_SCENARIO, SUM(TX_AMOUNT) AS total_amount
FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
GROUP BY TX_FRAUD_SCENARIO
ORDER BY TX_FRAUD_SCENARIO;

----------------------------------------------------------------------------------------------------------------------------------

-- Average ammount by fraud scenario

SELECT
    TX_FRAUD_SCENARIO,
    AVG(TX_AMOUNT) AS avg_amount
FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
GROUP BY TX_FRAUD_SCENARIO
ORDER BY avg_amount DESC;

----------------------------------------------------------------------------------------------------------------------------------

-- Top 10 customers with highest spends

SELECT TOP 10
    CUSTOMER_ID,
    SUM(TX_AMOUNT) AS total_spend,
    ROUND(SUM(TX_AMOUNT) * 1.0 / (SELECT SUM(CAST(TX_AMOUNT AS BIGINT)) FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD) * 100, 2) AS percentage_contribution
FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
GROUP BY CUSTOMER_ID
ORDER BY total_spend DESC;

------------------------------------------------------------------------------------------------------------------------