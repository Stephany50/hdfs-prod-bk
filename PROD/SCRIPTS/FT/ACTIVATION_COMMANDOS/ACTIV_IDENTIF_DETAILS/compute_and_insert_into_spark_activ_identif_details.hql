 INSERT INTO MON.SPARK_ACTIV_IDENTIF_DETAILS
 SELECT            a.IDENTIFIER_MSISDN,
                    a.MSISDN_IDENTIFIED,
                    CASE WHEN b.ACTIVATED_MSISDN IS NOT NULL THEN 1 ELSE 0 END AS EST_ACTIVES,   
                    (CASE WHEN b.DATE_ACTIVATION = to_date(a.DATE_IDENTIFICATION) 
                                                AND b.FIRST_DAY_REFILL = b.DATE_ACTIVATION
                                                AND b.ACTIVATED_MSISDN IS NOT NULL
                                            THEN 1 ELSE 0 END) AS EST_SUPER_ACTIVATION,   
                    (CASE WHEN b.DATE_ACTIVATION IS NOT NULL 
                                                AND (b.FIRST_DAY_REFILL IS NOT NULL OR 
                                                        ('###SLICE_VALUE###' = LAST_DAY('###SLICE_VALUE###') 
                                                         AND COALESCE(b.NEXT_MONTH_FIRST_DAY_REFIL_AMT, 0) > 0                                                         
                                                         AND DATE_ACTIVATION = '###SLICE_VALUE###') 
                                                     )
                                                AND b.ACTIVATED_MSISDN IS NOT NULL
                                            THEN 1 ELSE 0 END) AS EST_BONNES_ACTIVATION,   
                    (CASE WHEN b.DATE_ACTIVATION IS NULL
                                            THEN 1 ELSE 0 END) AS EST_BAD_ACTIVATION,   
                    (CASE WHEN b.DATE_ACTIVATION IS NOT NULL 
                                                AND (b.FIRST_DAY_REFILL IS NULL OR 
                                                        ('###SLICE_VALUE###' = LAST_DAY('###SLICE_VALUE###') 
                                                         AND COALESCE(b.NEXT_MONTH_FIRST_DAY_REFIL_AMT, 0) = 0                                                        
                                                         AND DATE_ACTIVATION = '###SLICE_VALUE###') 
                                                     )
                                                AND b.ACTIVATED_MSISDN IS NOT NULL
                                            THEN 1 ELSE 0 END) AS EST_DRY_ACTIVATION,
                    b.REFILL_COUNT AS RECHARGES,
                    CASE WHEN b.DATE_ACTIVATION = to_date(a.DATE_IDENTIFICATION) 
                                                AND b.FIRST_DAY_REFILL = b.DATE_ACTIVATION THEN b.FIRST_DAY_REFILL_AMOUNT ELSE 0 END AS RECHARGES_IMMEDIATES,
                    b.REFILL_AMOUNT AS RECHARGES_CUMULEES,
                    DATEDIFF(to_date(b.DATE_ACTIVATION),to_date(a.DATE_IDENTIFICATION)) AS DELAI, 
                    (CASE WHEN a.EST_SNAPPE = 'OUI' THEN 1 ELSE 0 END) AS EST_SNAPPES,                
                    (CASE WHEN b.DATE_ACTIVATION IS NOT NULL AND b.REFILL_AMOUNT >= 250
                                                AND b.ACTIVATED_MSISDN IS NOT NULL
                                            THEN 1 ELSE 0 END) AS EST_ACTIVATION_RECH_SUP_250,
                    (CASE WHEN a.EST_SNAPPE = 'OUI' 
                                                AND b.ACTIVATED_MSISDN IS NOT NULL THEN 1 ELSE 0 END) AS EST_ACTIVES_SNAPPES,  
                    (CASE WHEN a.EST_SNAPPE = 'OUI' AND b.DATE_ACTIVATION IS NOT NULL AND b.REFILL_AMOUNT >= 250
                                                AND b.ACTIVATED_MSISDN IS NOT NULL
                                            THEN 1 ELSE 0 END) AS EST_ACTIVES_SNAP_RECH_SUP_250,
                    CURRENT_TIMESTAMP AS INSERT_DATE ,
                    a.MOTIF_REJET ,
          '###SLICE_VALUE###' AS EVENT_DATE  
   FROM
                (
                    SELECT MSISDN AS MSISDN_IDENTIFIED,
                        IDENTIFICATEUR AS IDENTIFIER_MSISDN,
                        DATE_IDENTIFICATION,
                        EST_SNAPPE,
                        MOTIF_REJET
                    FROM DIM.SPARK_DT_BASE_IDENTIFICATION
                    WHERE DATE_IDENTIFICATION >= concat(substr('###SLICE_VALUE###',1,7),'-01')
                        AND DATE_IDENTIFICATION < date_sub('###SLICE_VALUE###',-1)
                ) a
                LEFT JOIN
                (    
                    SELECT T.MSISDN AS ACTIVATED_MSISDN,
                        REFILL_AMOUNT,
                        DATE_ACTIVATION,
                        FIRST_DAY_REFILL,
                        NEXT_MONTH_FIRST_DAY_REFIL_AMT,
                        REFILL_COUNT,
                        FIRST_DAY_REFILL_AMOUNT
                    FROM
                    (
                        SELECT MSISDN ,
                            COALESCE(REFILL_AMOUNT, 0) AS REFILL_AMOUNT,
                            ACTIVATION_DATE AS DATE_ACTIVATION,
                            FIRST_DAY_REFILL,
                            NEXT_MONTH_FIRST_DAY_REFIL_AMT,
                            REFILL_COUNT,
                            FIRST_DAY_REFILL_AMOUNT,
                            ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY ACTIVATION_DATE DESC) AS RG
                        FROM MON.SPARK_FT_ACTIVATION_MONTH1
                        WHERE EVENT_MONTH = substr('###SLICE_VALUE###',1,7)
                            AND ACTIVATION_DATE <= '###SLICE_VALUE###'
                    ) T
                    WHERE RG = 1 
                ) b
                ON a.MSISDN_IDENTIFIED = b.ACTIVATED_MSISDN;