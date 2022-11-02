SELECT USER_ID, MSISDN, NOM, PRENOM, NUMERO_CNI, DATE_NAISSANCE, ADDRESS 
from ( 
    select USER_ID, MSISDN, NOM, PRENOM, NUMERO_CNI, DATE_NAISSANCE, ADDRESS, ROW_NUMBER() OVER ( PARTITION BY MSISDN ORDER BY ADDRESS DESC ) as row_num 
    from (
        SELECT user_id USER_ID, A.msisdn MSISDN, user_last_name NOM, user_first_name PRENOM, id_number NUMERO_CNI, date_format(birth_date, 'yyyy-MM-dd') as DATE_NAISSANCE, CONCAT(B.adresse, '_', B.quartier, '_', B.ville) ADDRESS  
        from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW A 
        LEFT JOIN CDR.SPARK_IT_KYC_BDI_FULL B ON A.msisdn = B.msisdn 
        where event_date = "###SLICE_VALUE###"  
    ) C 
) D 
where D.row_num = 1 