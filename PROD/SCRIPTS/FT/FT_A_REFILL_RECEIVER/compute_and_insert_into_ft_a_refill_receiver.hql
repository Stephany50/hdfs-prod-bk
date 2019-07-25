insert into  mon.FT_A_REFILL_RECEIVER
     select
	 DATE_FORMAT(REFILL_DATE,'yyyy-MM') REFILL_MONTH
          , RECEIVER_MSISDN
          , REFILL_TYPE
          , REFILL_MEAN
          , RECEIVER_PROFILE
          , sum(REFILL_AMOUNT) REFILL_AMOUNT
          , sum(REFILL_BONUS) REFILL_BONUS
          , NULL REFILL_DISCOUNT
          , count(*) REFILL_COUNT
          , CURRENT_TIMESTAMP LAST_UPDATE
          , RECEIVER_OPERATOR_CODE operator_code
          ,REFILL_DATE
     from MON.FT_REFILL
     where refill_date ='###SLICE_VALUE###'
	 and termination_ind='200'
     and REFILL_MEAN<>'SCRATCH'
     group by
	 DATE_FORMAT(REFILL_DATE,'yyyy-MM')
     ,RECEIVER_MSISDN
     ,REFILL_TYPE
     ,REFILL_MEAN
     ,RECEIVER_PROFILE
     , RECEIVER_OPERATOR_CODE    ;
	 UNION ALL
     select
	 DATE_FORMAT(REFILL_DATE,'yyyy-MM') REFILL_MONTH
          , RECEIVER_MSISDN
          , REFILL_TYPE
          , REFILL_MEAN
          , RECEIVER_PROFILE
          , sum(REFILL_AMOUNT) REFILL_AMOUNT
          , sum(REFILL_BONUS) REFILL_BONUS
          , NULL REFILL_DISCOUNT
          , count(*) REFILL_COUNT
          , CURRENT_TIMESTAMP LAST_UPDATE
          , RECEIVER_OPERATOR_CODE OPERATOR_CODE
		  ,REFILL_DATE
     from MON.FT_REFILL
     where refill_date = '###SLICE_VALUE###'
	 and termination_ind='200'
     and REFILL_MEAN='SCRATCH'
     group by DATE_FORMAT(REFILL_DATE,'yyyy-MM')
     ,RECEIVER_MSISDN
     ,REFILL_TYPE
     ,REFILL_MEAN
     ,RECEIVER_PROFILE
     , RECEIVER_OPERATOR_CODE