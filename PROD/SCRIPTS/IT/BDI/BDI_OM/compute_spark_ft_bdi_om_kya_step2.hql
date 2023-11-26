INSERT INTO TMP.TT_BDI_OM_KYA_2
SELECT
    numeropiece,
    case 
        when number_phone= 1 then 'AE1' 
        when number_phone= 2 then 'AE2'
        when number_phone= 3 then 'AE3' 
        else 'OUI'
    end as est_multicompte_om
FROM
    (SELECT 
        numeropiece,
        count(telephone) number_phone
    FROM(
        SELECT DISTINCT
            numeropiece,
            telephone 
        FROM cdr.spark_it_kaabu_client_directory
        WHERE date_creation='###SLICE_VALUE###'
        union 
        SELECT DISTINCT
            numeropiece,
            telephone
        FROM 
        cdr.spark_it_nomad_client_directory 
        WHERE last_update_date='###SLICE_VALUE###' )P
    GROUP BY numeropiece)PP
