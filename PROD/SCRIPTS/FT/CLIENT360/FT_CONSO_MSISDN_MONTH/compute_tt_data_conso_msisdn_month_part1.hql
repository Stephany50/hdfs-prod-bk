insert into TMP.TT_DATA_CONSO_MSISDN_MONTH
        select * from MOM.FT_DATA_CONSO_MSISDN_MONTH
        where event_month = DATE_FORMAT(ADD_MONTHS(concat('###SLICE_VALUE###','-01'),-1) ,'yyyy-MM');
