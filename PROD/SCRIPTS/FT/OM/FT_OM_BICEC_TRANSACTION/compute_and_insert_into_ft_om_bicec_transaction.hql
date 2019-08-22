INSERT INTO MON.FT_OM_BICEC_TRANSACTION
--SELECT * FROM (
SELECT AGE, DEV, CHA, NCP, SUF, OPE, UTI, CLC, DCO, DVA, MON, SEN, LIB

, CONCAT('OM', LPAD (OPE, 3, '0'), LPAD (N_ROWNUM, 6, '0'))  PIE

, MAR, AGSA, AGEM, AGDE, DEVC, MCTV

, CONCAT('OM', LPAD (OPE, 3, '0'), LPAD (N_ROWNUM, 6, '0')) PIEO

, USER_ID, NULL,INSERT_DATE, EVENT_DATE


FROM (  
        SELECT 
              ROW_NUMBER() OVER()  N_ROWNUM, AGE, DEV
            
            , CHA, NCP, SUF, OPE, UTI, CLC, DCO

            , DVA, SUM(MON) MON, SEN, LIB

            , MAR, AGSA, AGEM, AGDE

            , DEVC, SUM(MCTV) MCTV

            , EVENT_DATE,NULL USER_ID
            , CURRENT_TIMESTAMP insert_date

            FROM TMP.TTVMW_OM_BICEC_TRANS

            WHERE EVENT_DATE ='2019-06-20'

            GROUP BY 
            AGE, DEV, CHA, NCP, SUF  

            , OPE, UTI, CLC, DCO, DVA, SEN, LIB

            , MAR, AGSA, AGEM, AGDE, DEVC

            , EVENT_DATE, NULL

)Y   