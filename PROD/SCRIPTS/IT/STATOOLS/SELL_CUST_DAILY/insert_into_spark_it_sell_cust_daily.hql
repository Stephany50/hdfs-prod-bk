INSERT INTO CDR.SPARK_IT_SELL_CUST_DAILY
SELECT
    Ticket_Reference,
    Article_reference,
    Description_of_the_article,
    Customer_Code,
    Customer_Name,
    Organization_Code,
    Staff_CUID,
    Staff_Name,
    Quantity_Sold,
    Quantity_Delivered,
    Amount_of_the_sale,
    Ticket_number,
    ORIGINAL_FILE_NAME,
    CURRENT_TIMESTAMP() INSERT_DATE,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Date_of_the_transaction, 'yyyyMMdd')) TRANSACTION_DATE
FROM CDR.SPARK_TT_SELL_CUST_DAILY C
LEFT JOIN (
    SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_SELL_CUST_DAILY
    WHERE TRANSACTION_DATE >= DATE_SUB(CURRENT_DATE,3)
) T ON (T.FILE_NAME = C.ORIGINAL_FILE_NAME)
WHERE T.FILE_NAME IS NULL