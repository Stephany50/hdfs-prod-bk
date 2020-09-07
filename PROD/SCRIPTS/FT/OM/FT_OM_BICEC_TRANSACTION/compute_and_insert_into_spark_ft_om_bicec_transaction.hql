INSERT INTO TABLE MON.SPARK_FT_OM_BICEC_TRANSACTION   PARTITION(EVENT_DATE)

SELECT AGE, DEV, CHA, NCP, SUF, OPE, UTI, CLC, DCO, DVA, MON, SEN, LIB

, CONCAT('OM', LPAD (OPE, 3, '0'), LPAD (N_ROWNUM, 6, '0'))  PIE

, MAR, AGSA, AGEM, AGDE, DEVC, MCTV

, CONCAT('OM', LPAD (OPE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO

, USER_ID, NULL,INSERT_DATE, EVENT_DATE


FROM (  
        SELECT 
              ROW_NUMBER() OVER(ORDER BY USER_ID)  N_ROWNUM, AGE, DEV
            
            , CHA, NCP, SUF, OPE, UTI, CLC, DCO

            , DVA, SUM(MON) MON, SEN, LIB

            , MAR, AGSA, AGEM, AGDE

            , DEVC, SUM(MCTV) MCTV

            , EVENT_DATE,NULL USER_ID
            , CURRENT_TIMESTAMP insert_date

            FROM MON.SPARK_TTVMW_OM_BICEC_TRANS

            WHERE EVENT_DATE ='###SLICE_VALUE###'

            GROUP BY 
            AGE, DEV, CHA, NCP, SUF  

            , OPE, UTI, CLC, DCO, DVA, SEN, LIB

            , MAR, AGSA, AGEM, AGDE, DEVC

            , EVENT_DATE, USER_ID

)Y   