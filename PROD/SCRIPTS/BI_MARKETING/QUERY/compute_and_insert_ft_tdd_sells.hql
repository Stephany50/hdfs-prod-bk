INSERT INTO MON.SPARK_FT_TDD_BOX_SELLS 
    SELECT 
        A.imei,
        inventory_name,
        type_box,
        invoice_number,
        store_description,
        payment_type,
        receipt_amount,            
        partenaire,
        current_timestamp insert_date,
        sale_date
    FROM 
    (
        SELECT DISTINCT *
        FROM MON.SPARK_FT_HOME_DAILY_SOLD 
        WHERE sale_date = '###SLICE_VALUE###'
    ) A 
    LEFT JOIN 
    (
        SELECT DISTINCT *
        FROM DIM.REF_ENLEVEMENT_BOX 
    ) B 
    ON TRIM(LEFT(A.imei, 14)) = TRIM(LEFT(B.imei, 14))