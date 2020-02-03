SELECT
     CONCAT('En MF ',DATE_FORMAT('2020-01-11','dd/MM')
    ,(case when cash_in>0 then '\n' || ' -Cash In ' || cash_in else '' end)
    ,(case when cash_out>0 then '\n' || ' -Cash Out '|| cash_out else '' end)
    ,(case when p2p>0 then '\n' || ' -P2P '|| p2p else '' end)
    ,(case when recharg>0 then '\n' || ' -Recharg '|| recharg else '' end)
    ,(case when march_pay>0 then '\n' || ' -March Pay '|| march_pay else '' end)
    ,(case when revenu_om>0 then '\n' || ' -Rev '|| revenu_om else '' end)) sms
FROM (
    SELECT
         round(max(cash_in)/1000000,0)cash_in
        , round(max(cash_out)/1000000,0)cash_out
        , round(max(p2p)/1000000,0)p2p
        , round(max(recharg)/1000000,0)recharg
        , round(max(march_pay)/1000000,0)march_pay
        , round(max(revenu_om)/1000000,2)revenu_om
    FROM(
        select
            '2020-01-11' tdate
            ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%out%' then amount else 0 end) cash_out
            ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%in%' then amount else 0 end) cash_in
            ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%chg_p2p%' then amount else 0 end) p2p
            ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%amt_rc%' then amount else 0 end) recharg
            ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%amt_merchpay%' then amount else 0 end) march_pay
            ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om
        from (
                        
            SELECT
                IF(SERVICE_TYPE='NULL',NULL,SERVICE_TYPE) SERVICE_TYPE -- workaround la requete interne avec les null ne marche pas
                , IF(SERVICE_GROUP_CODE='NULL',NULL,SERVICE_GROUP_CODE) SERVICE_GROUP_CODE
                , IF(SENDER_CATEGORY_CODE='NULL',NULL,SENDER_CATEGORY_CODE) SENDER_CATEGORY_CODE
                , IF(RECEIVER_CATEGORY_CODE='NULL',NULL,RECEIVER_CATEGORY_CODE) RECEIVER_CATEGORY_CODE
                , IF(SENDER_MSISDN='NULL',NULL,SENDER_MSISDN) SENDER_MSISDN
                , IF(RECEIVER_MSISDN='NULL',NULL,RECEIVER_MSISDN) RECEIVER_MSISDN
                , IF(RECEIVER_GRADE_NAME='NULL',NULL,RECEIVER_GRADE_NAME) RECEIVER_GRADE_NAME
                , IF(SENDER_GRADE_NAME='NULL',NULL,SENDER_GRADE_NAME) SENDER_GRADE_NAME
                , ROUND(AMOUNT,0) AMOUNT
                , INSERT_DATE
                , EVENT_DATE
            FROM (
                SELECT
                    SERVICE_TYPE
                    , 'AMT_ENT_OTHER'  SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL'  SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='ENT2REG'
                    And SENDER_MSISDN NOT IN ('690021088')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
                --#############################################       Bill payments (ENEO, Activa, CDE,¿)- Montant_transfert      (  AMT_BILLPAY_OTHER)           ##########################################################
                UNION ALL
            
                SELECT
                    SERVICE_TYPE
                    ,'AMT_BILLPAY_OTHER' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND ((TRANSFER_DONE = 'Y') AND ((SERVICE_TYPE)='BILLPAY'))
                    AND RECEIVER_MSISDN NOT IN ('orang')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_MSISDN
            
                --#############################################    Bill payments (ENEO, Activa, CDE,¿)-Revenus    (    CHG_BILLPAY      )        ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    , 'CHG_BILLPAY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE transfer_datetime ='2020-01-11'
                AND ((TRANSFER_DONE = 'Y') AND ((SERVICE_TYPE)='BILLPAY'))
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_MSISDN
            
                --#############################################        Bill payments OCM/OCMS- Montant_transfert     ( AMT_BILLPAY_OCM    )        ##########################################################
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_BILLPAY_OCM' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL'  RECEIVER_CATEGORY_CODE
                    , 'NULL'  SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND ((TRANSFER_DONE = 'Y') AND ((SERVICE_TYPE)='BILLPAY'))
                    AND RECEIVER_MSISDN ='orang'
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_MSISDN
            
            
                --#############################################       Bulk payment DF-Montant_transfert      AMT_BULK_DF             ##########################################################
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_BULK_DF' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('656980884')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
                    , SENDER_MSISDN
            
            
                --#############################################     Cash-in (Agence Partner)-Montant_transfert       AMT_CASHIN_AGENCY_PT              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_AGENCY_PT' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='O2C'
                    And RECEIVER_CATEGORY_CODE NOT IN  ('Enterprise','OAGENCE','OSHOP')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################     Cash-in (Agence)-Montant_transfert       AMT_CASHIN_AGENCY              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_AGENCY' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='CASHIN'
                    And SENDER_CATEGORY_CODE In ('OAGENCE','OSHOP')
                    And SENDER_MSISDN Not In ('656980884','690021071','697333649','656981578','697341791','697341843','697341906','656981559')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
            
                --#############################################    Cash-in (B2W)- Revenus         CHG_CASHIN_B2W             ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_CASHIN_B2W' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHIN')
                    And SENDER_CATEGORY_CODE In ('B2WENFANT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
            
                --#############################################    Cash-in (B2W)-Commision_charges   COMM_CASHIN_B2W                    ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHIN_B2W' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(COMMISSIONS_PAID) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='CASHIN'
                    And SENDER_CATEGORY_CODE In ('B2WENFANT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
                --#############################################     Cash-in (B2W)-Montant_transfert    AMT_CASHIN_B2W                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_B2W' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND SERVICE_TYPE='CASHIN'
                    AND SENDER_CATEGORY_CODE IN ('B2WENFANT')
                    AND SENDER_GRADE_NAME IN ('B2WAfrilandPrincipalGrade','B2WEcoBankPrincipalGrade')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
                    , SENDER_GRADE_NAME
            
            
                --#############################################      Cash-in (Entreprise)-Montant_transfert         AMT_CASHIN_ENT           ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_ENT' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='O2C'
                    And RECEIVER_CATEGORY_CODE ='Enterprise'
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
                --#############################################   Cash-in (hors Agence)-Commision_charges       COMM_CASHIN_OUT_AGENCY                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHIN_OUT_AGENCY' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(COMMISSIONS_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='CASHIN'
                    And SENDER_CATEGORY_CODE In ('IHB1','IHB2','INDB1','KAB1','KAPR1','KAR1','ORT1','ORT2')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
                --#############################################       Cash-in (hors Agence)-Montant_transfert        AMT_CASHIN_OUT_AGENCY           ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_OUT_AGENCY' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                     AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='CASHIN'
                    And SENDER_CATEGORY_CODE In ('IHB1','IHB2','INDB1','KAB1','KAPR1','KAR1','ORT1','ORT2','TOTALC','TOTALSTAT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
                --#############################################     Cash-in (Total)-Commision_charges         COMM_CASHIN_TOTAL            ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHIN_TOTAL' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(COMMISSIONS_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                     AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE='CASHIN'
                    And SENDER_CATEGORY_CODE In ('TOTALC','TOTALSTAT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
                --#############################################    Cash-in Marketing-Montant_transfert         AMT_CASHIN_MKT             ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHIN_MKT' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('690398450')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
                    , SENDER_MSISDN
            
            
            
                --#############################################       Cash-out (Agence)- Montant-transfert    AMT_CASHOUT_AGENCY               ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHOUT_AGENCY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE In ('OSHOP')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################     Cash-out (Agence)- Revenus       CHG_CASHOUT_AGENCY              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_CASHOUT_AGENCY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','P2PNONREG')
                    And RECEIVER_CATEGORY_CODE In ('OSHOP','OAGENCE')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################     Cash-out (B2W)- Revenus         CHG_CASHOUT_B2W            ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_CASHOUT_B2W' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE IN ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE IN ('B2WENFANT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################      Cash-out (B2W)-Commision_charges        COMM_CASHOUT_B2W            ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHOUT_B2W' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(COMMISSIONS_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND SERVICE_TYPE='CASHOUT'
                    AND RECEIVER_CATEGORY_CODE In ('B2WENFANT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################     Cash-Out (B2W)-Montant_transfert        AMT_CASHOUT_B2W             ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHOUT_B2W' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  ((TRANSFER_DONE = 'Y')
                    AND ((SERVICE_TYPE) In ('CASHOUT'))
                    AND ((RECEIVER_CATEGORY_CODE) In ('B2WENFANT')))
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
                    , RECEIVER_GRADE_NAME
            
                --#############################################     Cash-out (hors Agence)- Montant-transfert       AMT_CASHOUT_OUT_AGENCY              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHOUT_OUT_AGENCY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE In ('IHB1','IHB2','INDB1','KAB1','KAPR1','KAR1','ORT1','ORT2','TOTALC','TOTALSTAT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
            
                --#############################################      Cash-out (hors Agence)- Revenus         CHG_CASHOUT_OUT_AGENCY           ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_CASHOUT_OUT_AGENCY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE In ('IHB1','IHB2','INDB1','KAB1','KAPR1','KAR1','ORT1','ORT2','TOTALC','TOTALSTAT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
            
                --#############################################     Cash-out (hors Agence)-Commission_charges     COMM_CASHOUT_OUT_AGENCY                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHOUT_OUT_AGENCY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , sum(COMMISSIONS_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE In ('IHB1','IHB2','INDB1','KAB1','KAPR1','KAR1','ORT1','ORT2')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
            
                --#############################################    Cash-out (Total)-Commision_charges      COMM_CASHOUT_TOTAL                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'COMM_CASHOUT_TOTAL' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , sum(COMMISSIONS_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SERVICE_TYPE in ('CASHOUT','COUTBYCODE')
                    And RECEIVER_CATEGORY_CODE In ('TOTALC','TOTALSTAT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
            
                --#############################################    Frais de mission-Montant_transfert         AMT_MISSION             ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_MISSION' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('690021071','697333649','656981578','697341791','697341843','697341906','656981559')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
                    , SENDER_MSISDN
            
            
            
                --#############################################     Merchand payments - Montant-transfert          AMT_MERCHPAY         ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_MERCHPAY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  ((TRANSFER_DONE = 'Y')
                    AND ((SERVICE_TYPE)='MERCHPAY'))
                    AND RECEIVER_GRADE_NAME NOT LIKE  '%restau%'
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
            
                --#############################################      Merchand payments - Revenus         CHG_MERCHPAY          ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_MERCHPAY' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  ((TRANSFER_DONE = 'Y')
                    AND ((SERVICE_TYPE)='MERCHPAY'))
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
            
            
            
                --#############################################     Money transfer to non registered- Revenus    CHG_P2PNONREG                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_P2PNONREG' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND SERVICE_TYPE='P2PNONREG'
                GROUP BY
                    transfer_datetime
                    ,  SERVICE_TYPE
            
            
                --#############################################     restitution Money transfer to non registered- Revenus    CHG_P2PNONREG_ROLLBACK              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_P2PNONREG_ROLLBACK' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , -SUM(SERVICE_CHARGE_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND   SERVICE_TYPE='ROLLBACK' AND TRANSACTION_TAG='REMBOURSEMENT P2P NON ORANGE'
                GROUP BY
                    transfer_datetime
                    ,  SERVICE_TYPE
            
            
            
                --#############################################     restitution Money transfer On The Fly Revenus    CHG_P2PNONREG_ROLLBACK              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_ONTHEFLY_ROLLBACK' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , -SUM(SERVICE_CHARGE_PAID)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND   SERVICE_TYPE='ROLLBACK' AND TRANSACTION_TAG='REMBOURSEMENT P2P ON THE FLY'
                GROUP BY
                    transfer_datetime
                    ,  SERVICE_TYPE
            
            
                --#############################################     Money transfer to registered- Revenus    CHG_P2P               ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_P2P' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    AND SERVICE_TYPE='P2P'
                GROUP BY
                    transfer_datetime
                    ,  SERVICE_TYPE
            
            
                --#############################################   Payroll salary-Montant_transfert    AMT_ENT2REG                   ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_ENT2REG' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('690021088')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
                    , SENDER_MSISDN
            
            
            
                --#############################################    Pretups (e-Recharge) - Montant-transfert        AMT_RC              ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_RC' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                     AND  ((TRANSFER_DONE = 'Y') AND ((SERVICE_TYPE)='RC'))
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
                --#############################################    Retraits distributeurs- Montant-transfert   AMT_OPT                   ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_OPT' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And RECEIVER_CATEGORY_CODE In ('OPT')
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_CATEGORY_CODE
            
            
                --#############################################     Ticket Restau - Montant-transfert          AMT_MERCHPAY_RESTAU          ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_MERCHPAY_RESTAU' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                     AND  ((TRANSFER_DONE = 'Y') AND ((SERVICE_TYPE)='MERCHPAY')) AND RECEIVER_GRADE_NAME like  '%restau%'
                GROUP BY
                    transfer_datetime
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
                --#############################################     Merchant payments (Carte VISA)  :   VISA_POS_VISA_ATM (VISA)- Montant          AMT_MERCHPAY_VISA        ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_MERCHPAY_VISA' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM (TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And RECEIVER_MSISDN In ('656981241')
                    And RECEIVER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    , RECEIVER_MSISDN
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
                --#############################################    Cash-out (Carte VISA):    VISA_POS_VISA_ATM (VISA)- Montant          AMT_CASHOUT_VISA        ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_CASHOUT_VISA' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM (TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And RECEIVER_MSISDN In ('656981242')
                    And RECEIVER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    , RECEIVER_MSISDN
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
            
                --#############################################  Merchant payments (Carte VISA)     VISA_POS_VISA_ATM (VISA)- Revenu          CHG_MERCHPAY_VISA        ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_MERCHPAY_VISA' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM (SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And RECEIVER_MSISDN In ('656981241')
                    And RECEIVER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    ,  RECEIVER_MSISDN
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
            
                --#############################################   Cash-out (Carte VISA)    VISA_POS_VISA_ATM (VISA)- Revenu          CHG_CASHOUT_VISA        ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_CASHOUT_VISA' SERVICE_GROUP_CODE
                    , 'NULL' SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , 'NULL' SENDER_MSISDN
                    , RECEIVER_MSISDN
                    , RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM (SERVICE_CHARGE_RECEIVED)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And RECEIVER_MSISDN In ('656981242')
                    And RECEIVER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    , RECEIVER_MSISDN
                    , SERVICE_TYPE
                    , RECEIVER_GRADE_NAME
            
            
            
                --#############################################      VISA_REVERSAL_VISA_REFUND (VISA)- Restitution  Montant     AMT_VISA_REFUND             ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'AMT_VISA_REFUND' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM (TRANSACTION_AMOUNT)  AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('656981244','656981243')
                    And SENDER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    ,  SENDER_MSISDN
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            
            
                --#############################################      VISA_REVERSAL_VISA_REFUND (VISA)- Restitution  revenu  CHG_VISA_REFUND                ##########################################################
            
                UNION ALL
                SELECT
                    SERVICE_TYPE
                    ,'CHG_VISA_REFUND' SERVICE_GROUP_CODE
                    , SENDER_CATEGORY_CODE
                    , 'NULL' RECEIVER_CATEGORY_CODE
                    , SENDER_MSISDN
                    , 'NULL' RECEIVER_MSISDN
                    , 'NULL' RECEIVER_GRADE_NAME
                    , 'NULL' SENDER_GRADE_NAME
                    , SUM(SERVICE_CHARGE_RECEIVED) AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    , transfer_datetime EVENT_DATE
                FROM CDR.IT_OMNY_TRANSACTIONS
                WHERE
                    transfer_datetime ='2020-01-11'
                    AND  TRANSFER_DONE = 'Y'
                    And SENDER_MSISDN In ('656981244','656981243')
                    And SENDER_CATEGORY_CODE='B2WCOMMIS'
                GROUP BY
                     transfer_datetime
                    , SENDER_MSISDN
                    , SERVICE_TYPE
                    , SENDER_CATEGORY_CODE
            )T
        ) a
        LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
        where event_date = '2020-01-11'
        group by event_date
    )omny
)t