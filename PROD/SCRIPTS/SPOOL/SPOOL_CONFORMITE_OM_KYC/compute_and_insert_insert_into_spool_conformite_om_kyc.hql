INSERT INTO SPOOL.SPOOL_CONFORMITE_OM_KYC

SELECT

NOM,
PRENOM,
DATE_NAISSANCE,
NUMERO_CNI,
MSISDN,
EST_ACTIF_OM,
Activation_date_TEL,
Created_date_OM,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_DATE

FROM

    (SELECT
        A.USER_LAST_NAME AS NOM,
        A.USER_FIRST_NAME AS PRENOM,
        A.BIRTH_DATE AS DATE_NAISSANCE,
        A.ID_NUMBER AS NUMERO_CNI,
        A.MSISDN AS MSISDN,
        case when B.msisdn is not null then 'OUI'else 'NON' end AS EST_ACTIF_OM,
        A.Activation_date_TEL AS Activation_date_TEL,
        A.REGISTERED_ON AS Created_date_OM

    FROM
        (
        SELECT
            a.*,
            b.activation_date Activation_date_TEL

            FROM
                (
                SELECT
                    a.*,
                    row_number() OVER (PARTITION BY a.Msisdn ORDER BY REGISTERED_ON DESC) rn
                from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT a
                WHERE EVENT_DATE ='###SLICE_VALUE###'
                )a

                CROSS JOIN

                MON.SPARK_FT_CONTRACT_SNAPSHOT b

        WHERE
            a.MSISDN in
            (
              select MSISDN TELEPHONE
              from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
              where EVENT_DATE='###SLICE_VALUE###' and user_domain='Subscriber'
             )
             and a.MSISDN=b.ACCESS_KEY
             and a.EVENT_DATE=b.EVENT_DATE
             and a.EVENT_DATE='###SLICE_VALUE###'
             and rn=1
      
        ) A
        LEFT JOIN
        (
        SELECT
            distinct msisdn
        from MON.SPARK_FT_OM_ACTIVE_USER
        where event_date in (date_sub(current_date, 1) ,date_sub(current_date, 30),date_sub(current_date, 60))

        ) B
        ON A.msisdn = B.msisdn
    ) TTT

