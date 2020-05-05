 merge into MON.SPARK_FT_STKAPRIM_REFILL_TYPE a
        using 
        (
            select a.msisdn, a.activ_begin_date, a.activ_end_date, b.grdparent, b.partner_name, b.activ_begin_date as parent_begin_date, b.activ_end_date as parent_end_date
            from dim.dim_stk_hierachy a
            inner join dim.dim_stk_hierachy b
            on( a.grdparent = b.msisdn
            and a.activ_begin_date >= b.activ_begin_date 
            and a.activ_begin_date < nvl(b.activ_end_date, current_timestamp ))
             and refill_date '###SLICE_VALUE###'
        and  '###SLICE_VALUE###' >= activ_begin_date
        and '###SLICE_VALUE###' < nvl(activ_end_date, current_timestamp)
        ) b
        on ( a.stkaprim = b.msisdn)

        )
        when matched then 
        update set a.partenaire = b.partner_name