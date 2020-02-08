merge into TMP.TT_FT_IMEI_ONLINE D
using
(

        select

        imei,
        msisdn,
        count(*) NBRE,
        transaction_date sdate
        from MON.FT_OTARIE_DATA_TRAFFIC_DAY
        where transaction_date = '###SLICE_VALUE###'
        group by
        transaction_date,
        msisdn,
        imei

) S
on (D.SDATE = S.SDATE
    AND substr(D.IMEI, 1, 8) = substr(S.IMEI, 1, 8)

    AND D.MSISDN = S.MSISDN
    )
when matched then
    update set src_table = CONCAT(src_table, 'OTARIE|')
when not matched then
    insert
        values(S.IMEI, D.IMSI, S.MSISDN, NBRE, 'OTARIE|', CURRENT_TIMESTAMP, S.SDATE)
