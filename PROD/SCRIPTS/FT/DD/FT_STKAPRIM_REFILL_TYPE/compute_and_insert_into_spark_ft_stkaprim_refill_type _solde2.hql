insert into TMP.SPARK_FT_STKAPRIM_REFILL_TYPE
       select
	   stkaprim,
	   partenaire,
	   type_event,
	   event_simplifie,
	   amount,
	   commission,
	   null capilarite,
	   null capat_cumul,
	   Source,
	   ,insert_date
	   '###SLICE_VALUE###' refill_date
	   from
       (
           select refill_date
              , case when parent_to_consider = 'MSISDN' then MSISDN
                   when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
                   else nvl(parent, grdparent)
                   end stkaprim
              ,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
                   when parent_to_consider = 'PARENT' then 'Solde_Route'
                   else 'Solde_Route'
                   end Type_Event
              , 'Solde' Event_Simplifie
              , sum(amount) amount
              , 'IT_ZEBRA_MASTER_BAL_SOLDE'  Source
              , CURRENT_TIMESTAMP AS INSERT_DATE
           from
           (
               select '###SLICE_VALUE###' as refill_date, mobile_number as msisdn, category_code, parent, grdparent, AVAILABLE_BALANCE/100 as amount
               from
               (
               select *    --count(*)
               from CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
               where event_date = DATE_SUB('###SLICE_VALUE###',1)    --'06/01/2020'
               and event_time = '22'
               ) a
               LEFT JOIN (select * from DIM.DIM_STK_HIERACHY
               where '###SLICE_VALUE###' >= activ_begin_date
               and '###SLICE_VALUE###' < nvl(activ_end_date, '2025-12-31' )
               ) b
               ON a.mobile_number = b.msisdn
           ) T
           LEFT JOIN
           (select distinct sender_category, parent_to_consider from dim.dt_send_rec_cat_refill ) c
           ON T.category_code = c.sender_category
           group by
               refill_date
              , case when parent_to_consider = 'MSISDN' then MSISDN
                   when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
                   else nvl(parent, grdparent)
                   end
              ,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
                   when parent_to_consider = 'PARENT' then 'Solde_Route'
                   else 'Solde_Route'
                   end
       ) a
       LEFT JOIN ( select distinct stkaprim, source  from TMP.TT_STKAPRIM_REFILL_TYPE )b
       ON a.STKAPRIM = b.stkaprim
           and amount > 0
           and b.source is null