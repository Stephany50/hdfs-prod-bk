insert into TMP.TT_DATA_CONSO_MSISDN_MONTH
        select * from FT_DATA_CONSO_MSISDN_MONTH
        where event_month = DATE_FORMAT(ADD_MONTHS('###SLICE_VALUE###',-1) ,'yyyy-MM');