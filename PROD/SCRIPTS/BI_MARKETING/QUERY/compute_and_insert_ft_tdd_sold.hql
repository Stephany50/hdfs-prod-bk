INSERT INTO MON.SPARK_FT_HOME_DAILY_SOLD
    SELECT
        DISTINCT A.inventory_name,
        B.type_box,
        UPPER(TRIM(LEFT(A.sn, 14))) AS imei,
        A.invoice_number,
        A.store_description,
        A.payment_type,
        A.receipt_amount,
        A.stat_date AS sale_date
    FROM
        (
            SELECT
                inventory_name,
                sn,
                stat_date,
                invoice_number,
                store_description,
                payment_type,
                receipt_amount
            FROM CDR.SPARK_IT_DAILY_EQUIMENT_SOLD
            WHERE stat_date = '###SLICE_VALUE###'
            GROUP BY
                inventory_name,
                sn,
                stat_date,
                invoice_number,
                store_description,
                payment_type,
                receipt_amount
        ) A 
        INNER JOIN
        (
            SELECT * FROM DIM.DT_HOME_FLYBOX
        ) B 
        ON TRIM(UPPER(A.inventory_name)) = TRIM(UPPER(B.inventory_name))