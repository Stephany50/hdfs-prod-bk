 merge into FT_DATA_CONSO_MSISDN_MONTH   a
        using
        (
            select *
            from TMP.TT_DATA_CONSO_MSISDN_MONTH
        )  b
        on ( a.event_month = '###SLICE_VALUE###'
            and a.msisdn = b.msisdn
            )
        when matched then update set
             a.MAX_BYTES_USED = case when nvl(a.bytes_sent+a.bytes_received, 0) >= nvl(b.MAX_BYTES_USED, 0) then nvl(a.bytes_sent+a.bytes_received, 0) else nvl(b.MAX_BYTES_USED, 0) end
            , a.MONTH_MAX_BYTES_USED = case when nvl(a.bytes_sent+a.bytes_received, 0) >= nvl(b.MAX_BYTES_USED, 0) then a.EVENT_MONTH else b.MONTH_MAX_BYTES_USED  end
        when not matched then
            values(b.msisdn, b.commercial_offer, b.FIRST_ACTIVE_DAY, b.LAST_ACTIVE_DAY, b.MAX_BYTES_USED, b.MONTH_MAX_BYTES_USED,'###SLICE_VALUE###') ;
