merge into TMP.TT_FT_IMEI_ONLINE D
using
(

        SELECT
             SERVED_PARTY_IMEI IMEI
            , SERVED_PARTY_IMSI IMSI
            , SERVED_PARTY_MSISDN MSISDN
            , SUM(1) nbre
            , SESSION_DATE SDATE
        FROM MON.SPARK_FT_CRA_GPRS
        WHERE
              SESSION_DATE = '###SLICE_VALUE###'
              AND SERVED_PARTY_IMEI IS NOT NULL
        GROUP BY SESSION_DATE
            , SERVED_PARTY_IMEI
            , SERVED_PARTY_IMSI
            , SERVED_PARTY_MSISDN

) S
on (D.SDATE = S.SDATE
    AND D.IMEI = S.IMEI
    AND D.IMSI = S.IMSI
    AND D.MSISDN = S.MSISDN
    )
when matched then
    update set src_table = concat(src_table,'GPRS|')
when not matched then
    insert
        values(S.IMEI, S.IMSI, S.MSISDN, NBRE, 'GPRS|', CURRENT_TIMESTAMP, S.SDATE)