CREATE DATABASE BANK;
  
USE Bank;

CREATE TABLE Transactions (
    step INT,
    type VARCHAR(50),
    amount DECIMAL(15, 2),
    nameOrig VARCHAR(255),
    oldbalanceOrg DECIMAL(15, 2),
    newbalanceOrig DECIMAL(15, 2),
    nameDest VARCHAR(255),
    oldbalanceDest DECIMAL(15, 2),
    newbalanceDest DECIMAL(15, 2),
    isFraud TINYINT,
    isFlaggedFraud TINYINT
);

SELECT * FROM transactions;

show variables like 'local_infile';

SET GLOBAL local_inflie=1;

LOAD DATA LOCAL INFILE "C:/Users/user/Desktop/Transactions PS_20174392719_1491204439457_log.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM transactions;

-- 1. total number of transactions
SELECT COUNT(*) AS total_transactions
FROM transactions;
-- (Interpretation: The query result shows a total of X transactions in the database. This value reflects the number of transactions recorded for the specified period.)

-- 2. Order by transaction amount in descending order
SELECT * FROM Transactions
ORDER BY amount DESC
LIMIT 10;
-- (Interpretation: The query highlights the top 10 transactions with the highest monetary values. These transactions account for a significant portion of the dataset's total amount and may represent high-value customers, large purchases, or potential anomalies.) 

-- 3. Group by transaction type and count the number of transactions
SELECT type, COUNT(*) AS transaction_count
FROM Transactions
GROUP BY type; 
-- (Interpretation: The query results provide a breakdown of transactions by type, revealing the distribution across categories such as [e.g., "purchases," "refunds," "transfers"]. This information highlights which transaction types are most common and which are less frequent.)

-- 4. Filter transactions where the amount is between 1000 and 5000
SELECT * FROM Transactions
WHERE amount BETWEEN 1000 AND 5000;
-- (Interpretation: The query identifies transactions with amounts between 1,000 and 5,000 units. These mid-range transactions may represent [specific types of activity, e.g., standard purchases, recurring payments, or medium-sized business transactions].)

-- 5. Find transactions where fraud occurred and order by step
SELECT * FROM Transactions
WHERE isFraud = 1
ORDER BY step;
-- (Interpretation: The query identifies transactions flagged as fraudulent, sorted by the step column. These transactions provide insights into the timing and nature of fraudulent activities. Key observations might include clusters of fraud during specific periods, recurring transaction patterns, or high-risk transaction attributes.) 

-- 6. Common Table Expression (CTE) for high-value transactions
WITH HighValueTransactions AS (
    SELECT *
    FROM Transactions
    WHERE amount > 100000
)
SELECT *
FROM HighValueTransactions
WHERE isFraud = 1;
-- (Interpretation: The query identifies high-value transactions (above 100,000 units) and flags those that are marked as fraudulent (isFraud = 1). This subset represents a critical area of concern for the business, as high-value fraudulent transactions can lead to significant financial losses and reputational damage. These transactions should be examined closely to understand the scope of fraud.)

-- 7. Use of HAVING to filter grouped data
SELECT type, AVG(amount) AS avg_amount
FROM Transactions
GROUP BY type
HAVING AVG(amount) > 5000;
-- (Interpretation: The query identifies transaction types where the average transaction amount exceeds 5,000 units. These high-value transaction types represent business activities that involve significant sums, which may include premium customers, high-ticket items, or major financial transactions.)

-- 8. Subquery: Find transactions with an amount greater than the average
SELECT *
FROM Transactions
WHERE amount > (SELECT AVG(amount) FROM Transactions);
-- (Interpretation: This query retrieves transactions where the amount is greater than the average transaction amount. These transactions represent high-value activities that could be tied to premium customers or major purchases. Such transactions typically contribute to a substantial portion of revenue and are critical to the overall business.)
 
-- 9. Union: Combine two result sets (e.g., flagged and fraudulent transactions)
SELECT *
FROM Transactions
WHERE isFraud = 1
UNION
SELECT *
FROM Transactions
WHERE isFlaggedFraud = 1;
-- (Interpretation: The query identifies both confirmed fraudulent transactions (isFraud = 1) and transactions flagged as potentially fraudulent (isFlaggedFraud = 1), combining the two datasets for a comprehensive view of transactions that may involve fraud. This allows the business to focus on both transactions already confirmed as fraudulent and those still under investigation for suspicious activity.)

-- 10. Create a view for flagged or fraudulent transactions
CREATE OR REPLACE VIEW FlaggedOrFraudulent AS
SELECT *
FROM Transactions
WHERE isFraud = 1 OR isFlaggedFraud = 1;
-- (Interpretation: This query creates a view called FlaggedOrFraudulent that combines transactions identified as fraudulent (isFraud = 1) or flagged as potentially fraudulent (isFlaggedFraud = 1). The view provides an easy way to consistently access and analyze transactions that are suspected of fraud or have already been confirmed as fraudulent.) 

-- 11. Detecting Recursive Fraudulent Transactions(Use a recursive CTE to identify potential money laundering chains where money is transferred from one account to another across multiple steps, with all transactions flagged as fraudulent.)
WITH RECURSIVE fraud_chain as (
SELECT nameOrig as initial_account, nameDest as next_account, step, amount, newbalanceorig
FROM transactions
WHERE isFraud = 1 and type = 'TRANSFER'

UNION ALL 

SELECT fc.initial_account, t.nameDest, t.step,t.amount ,t.newbalanceorig
FROM fraud_chain fc
JOIN transactions t
ON fc.next_account = t.nameorig and fc.step < t.step 
where t.isfraud = 1 and t.type = 'TRANSFER')
SELECT * FROM fraud_chain;
-- (Interpretation: This query uses a recursive CTE to track the flow of money through multiple accounts over successive steps. The recursive part of the CTE allows us to follow the chain of transations and identify patterns that could indicate money laundering activites. It filters out chains where all transactions are marked as fraudulent.) 

-- 12. Analyzing fraudulent Activity over Time(use CTE to calculate the rolling sum of fraudlent transactions for each account over the last 5 steps. 
with rolling_fraud as ( SELECT nameorig,step, 
SUM(isfraud) OVER (PARTITION BY nameOrig order by STEP ROWS BETWEEN 4 PRECEDING and CURRENT ROW ) as fraud_rolling
FROM transactions)
SELECT * FROM rolling_fraud
WHERE fraud_rolling > 0 ;
-- (Interpretation: This query calculates a rolling sum of fraudulent transactions for each account (nameOrig) over a 5-step window. It uses the window function SUM(isfraud) OVER to accumulate the number of fraudulent transactions (isfraud = 1) within this window. The final result filters out only the rows where the rolling sum of frauds is greater than zero, meaning those accounts have experienced at least one fraudulent transaction within the last 5 steps.) 

-- 13. Using multiple CTEs to identify accounts with suspicious activity, including large transfer, consecutive transactions without balance change, and flagged transactions.
WITH large_transfers AS (
    SELECT nameOrig, step, amount
    FROM transactions
    WHERE type = 'TRANSFER' AND amount > 500000
),
no_balance_change AS (
    SELECT nameOrig, step, oldbalanceOrg, newbalanceOrig
    FROM transactions
    WHERE oldbalanceOrg = newbalanceOrig
),
flagged_transactions AS (
    SELECT nameOrig, step
    FROM transactions
    WHERE isFlaggedFraud = 1
)
SELECT lt.nameOrig
FROM large_transfers lt
JOIN no_balance_change nbc ON lt.nameOrig = nbc.nameOrig AND lt.step = nbc.step
JOIN flagged_transactions ft ON lt.nameOrig = ft.nameOrig AND lt.step = ft.step;
-- (Interpretation: The query identifies accounts involved in large transfers (greater than 500,000 units) where there is no change in the originating account balance and the transactions have been flagged as potentially fraudulent. This combination of factors makes these accounts particularly suspicious and worthy of investigation.)

-- 14. Write me a query that checks if the computed new_updated_Balance is the same as the actual newbalanceDest in the table. If they are equal, it returns those rows.
with CTE as (
SELECT amount,nameorig,oldbalancedest,newbalanceDest,(amount+oldbalancedest) as new_updated_Balance 
FROM transactions
)
SELECT * FROM CTE where new_updated_Balance = newbalanceDest;
-- (Interpretation: This query calculates the expected new balance for the destination account (nameDest) by adding the transfer amount (amount) to the original balance of the destination account (oldbalancedest). The query then checks if this expected updated balance (new_updated_Balance) matches the reported new balance (newbalanceDest). If the values match, it implies that the transaction was properly recorded and balanced, meaning there are no discrepancies in the transfer balance for the destination account.) 

-- 15. Find transactions where the destination account had zero balance before or after the transaction.
SELECT nameDest, step, amount, oldbalanceDest, newbalanceDest
FROM transactions
WHERE oldbalanceDest = 0 OR newbalanceDest = 0;
-- (Interpretation: This query calculates the expected new balance for the destination account (nameDest) by adding the transfer amount (amount) to the original balance of the destination account (oldbalancedest). The query then checks if this expected updated balance (new_updated_Balance) matches the reported new balance (newbalanceDest). If the values match, it implies that the transaction was properly recorded and balanced, meaning there are no discrepancies in the transfer balance for the destination account.) 

-- 16. Write a query to list transactions where oldbalanceDest or newbalanceDest is zero.
SELECT * FROM transactions
WHERE isFraud = 1;
-- (Interpretation: This query filters and retrieves all transactions that have been marked as fraudulent, where the isFraud flag is set to 1. The results provide detailed information about these transactions, including all associated fields (e.g., account names, amounts, transaction types, timestamps, etc.) that can be analyzed to understand the nature of the fraudulent activity.) 





