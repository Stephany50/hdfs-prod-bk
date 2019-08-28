set feedback off
set verify off
SET TERMOUT OFF
set trimspool on
set echo off
set pause off
set pages 10000
set lines 10000
set colsep ";"
set wrap off
SET linesize 999000
--  non affichage de l'entete
SET head off
--
-- forcer le format des dates
ALTER SESSION SET NLS_DATE_FORMAT = 'yyyymmdd hh24miss';
-- spool nom_du_fichier.extension
-- spool ###OUTPUT_FILEPATH###
-- ###FLUXLOGICAL_NAME###  , ###SQL_FILENAME_INFIX### , ###CURR_SLICE_VALUE### ,  ###OUTPUT_FILEPATH###
--
--
-- sample {0} = 20090708
-- Ø
	-- spool nom_du_fichier.extension
	spool ###OUTPUT_FILEPATH###
	--
	--
	-- preciser l'entete
	-- SELECT 'EVENT_DATE, ,' ROW_DATA FROM DUAL ;
	--
	-- || ';' ||

SELECT b.MSISDN||';'||'CA En MF '||tdate
||(case when C2S>0 then '\n' || ' -C2S '|| C2S else '' end)
||(case when AllRev>0 then '\n' || ' -CA Total  '|| AllRev ||',' else '' end)
||(case when ALLData>0 then '\n' || ' dont Data '|| ALLData || ',' else '' end)
||(case when SVA>0 then '\n' || ' SVA '|| SVA || ',' else '' end)
||(case when Others>0 then '\n' || ' Autres '|| Others else '' end)
||'\n'||' OM :'
||(case when cash_in>0 then '\n' || ' -Cash In '|| cash_in else '' end)
||(case when cash_out>0 then '\n' || ' -Cash Out '|| cash_out else '' end)
||(case when revenu_om>0 then '\n' || ' -Rev '|| revenu_om else '' end)
row_data
FROM (SELECT
      TO_CHAR (TRANSACTION_DATE , 'dd/mm/yy') tdate
      , round(max(case when nvl(a.C2S,0)=0 then rech.C2S else a.C2S end)/1000000,2)C2S
      , round(sum(case when TRAFFIC_MEAN='REVENUE' and SUB_ACCOUNT='MAIN' then TAXED_AMOUNT else 0 end)/1000000,2)AllRev
      , round(sum(case when usage_code in ('NVX_GPRS_PAYGO','NVX_USS') then TAXED_AMOUNT else 0 end)/1000000,2)ALLData
      , round((sum(case when usage_code ='NVX_GPRS_SVA' or usage_code in (select usage_code from dim.DT_USAGES where upper(PRODUCT) like '%OTHER%VAS%') then TAXED_AMOUNT else 0 end)+max(sva))/1000000,2)SVA
      , round(sum(case when TRAFFIC_MEAN='REVENUE'and SUB_ACCOUNT='MAIN' and usage_code not in ('NVX_GPRS_PAYGO','NVX_USS','NVX_GPRS_SVA') and usage_code not in (select usage_code from dim.DT_USAGES where upper(PRODUCT) like '%OTHER%VAS%') then TAXED_AMOUNT else 0 end)/1000000,2)Others
		, round(max(cash_in)/1000000,0)cash_in, round(max(cash_out)/1000000,0)cash_out, round(max(revenu_om)/1000000,2)revenu_om
      ,'' ROW_DATA
    FROM
      DIM.DT_USAGES,
      FT_GLOBAL_ACTIVITY_DAILY,
      (select REFILL_DATE,sum(REFILL_AMOUNT) c2s
        from ft_refill
        where REFILL_DATE=TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
        and REFILL_MEAN='C2S'
        and REFILL_TYPE = 'RC'
        and TERMINATION_IND = '200'
        group by REFILL_DATE ) a, --437 736 350
       (select TRANSACTION_DATE SDATE,
        SUM(DECODE(SUBSTR(t.DESTINATION,-3,3),'SVA',NVL(t.MAIN_RATED_AMOUNT,0)+NVL(t.PROMO_RATED_AMOUNT,0),0)) sva
        from FT_gsm_traffic_revenue_daily t
        where TRANSACTION_DATE = TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
        Group by TRANSACTION_DATE )sva, --1 137 448
        (select TRANSACTION_DATE RECH_DATE, sum(REFILL_AMOUNT)C2S
        from ft_recharge
        where TRANSACTION_DATE= TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
        and REFILL_CHANNEL='e-topup'
        and REFILL_BALANCE='Main Balance'
        group by TRANSACTION_DATE) rech,
		(select event_date SDATE
		,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%out%' then amount else 0 end) cash_out
		,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%in%' then amount else 0 end) cash_in
		,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om
		from FT_OMNY_GLOBAL_ACTIVITY a, dim.DT_OM_FINANCE_DASHBORD_USAGE b
		where event_date = TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
		and a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE(+)
		group by event_date)omny
    WHERE
      ( FT_GLOBAL_ACTIVITY_DAILY.SERVICE_CODE=DIM.DT_USAGES.USAGE_CODE(+)  )
      AND
      (FT_GLOBAL_ACTIVITY_DAILY.TRAFFIC_MEAN='REVENUE'
       AND FT_GLOBAL_ACTIVITY_DAILY.OPERATOR_CODE  ='OCM'
       AND FT_GLOBAL_ACTIVITY_DAILY.SUB_ACCOUNT ='MAIN'
       AND FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE  = TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
      ) AND FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE = a.REFILL_DATE (+)
	    AND FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE = rech.RECH_DATE(+)
        AND FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE = sva.SDATE
		 AND FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE = omny.SDATE
    GROUP BY TRANSACTION_DATE
) a, (select msisdn from DIM.DT_SMSNOTIFICATION_RECIPIENT
where type='FT_GSM_TRAFFIC_REVENUE_DAILY'  and actif = 'YES') b  ;


--
--
	-- compresser le fichier final TO_DATE ('###CURR_SLICE_VALUE###', 'yyyymmdd')
	spool off
	-- host "gzip -f C:\SPOOL\test_me.csv"
--
spool off
set feedback on
set verify on
set pause on
set echo ON
--
quit
