INSERT INTO AGG.FT_A_VAS_REVENUE_DAILY
        SELECT
           a.OTHER_PARTY   VAS_NUMBER,
					 NULL VAS_SHORT_NUMBER,
					 NULL VAS_LONG_NUMBER,
					 cast(HOUR (a.NQ_TRANSACTION_DATE) as string) TRANSACTION_HOUR,
					 UPPER (a.COMMERCIAL_PROFILE) COMMERCIAL_OFFER,
					 a.SPECIFIC_TARIFF_INDICATOR SPECIFIC_CHARGING_INDICATOR,
					 a.SERVICE_CODE SERVICE_CODE,
					 a.TELESERVICE_INDICATOR,
					 (CASE  WHEN  nvl(a.BILLING_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ) IS_BILLING_TERM_ERROR,
					 (CASE  WHEN  nvl(a.NETWORK_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ) IS_NETWORK_TERM_ERROR,
					 (CASE
                            WHEN concat(a.LOCATION_MCC,a.LOCATION_MNC) = '62402' THEN LPAD (CAST(CONV(a.LOCATION_LAC ,16,10) AS string), 5, 0)
                            ELSE NULL END  )  LOCATION_LAC,
                      SUM (1) TRANSACTION_COUNT,
					  SUM (CASE WHEN a.Main_Rated_Amount + a.Promo_Rated_Amount > 0 THEN NVL (a.CALL_PROCESS_TOTAL_DURATION, 0) ELSE 0 END) RATED_DURATION,
					  SUM (NVL (a.CALL_PROCESS_TOTAL_DURATION, 0) ) CALL_PROCESS_TOTAL_DURATION,
					  SUM ( CEIL  ( (CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN NVL (a.CALL_PROCESS_TOTAL_DURATION, 0) ELSE 0 END) / 60) ) RATED_DURATION_SPLIT_MINUTE,
					  SUM ( CEIL  ( NVL (a.CALL_PROCESS_TOTAL_DURATION, 0) / 60) )  PROCESS_DURATION_SPLIT_MINUTE,
					  SUM (NVL (a.RATED_VOLUME, 0) ) RATED_VOLUME,
					  SUM (NVL (a.MAIN_REFILL_AMOUNT, 0) ) MAIN_REFILL_AMOUNT,
					  SUM (NVL (a.BUNDLE_REFILL_AMOUNT, 0) ) BUNDLE_REFILL_AMOUNT,
					  SUM (NVL (a.MAIN_RATED_AMOUNT, 0) + NVL (a.PROMO_RATED_AMOUNT, 0) ) TOTAL_RATED_AMOUNT,
					  SUM (NVL (a.MAIN_RATED_AMOUNT, 0) ) MAIN_RATED_AMOUNT,
					  SUM (NVL (a.PROMO_RATED_AMOUNT, 0) ) PROMO_RATED_AMOUNT,
					  SUM (NVL (a.RATED_AMOUNT_IN_BUNDLE, 0) ) RATED_AMOUNT_IN_BUNDLE,
					  SUM (NVL (a.SMS_USED_VOLUME, 0) ) SMS_USED_VOLUME,
					  'IN' SOURCE_PLATEFORM,  'IN' SOURCE_DATA,
					  SUM (CASE WHEN NVL (a.MAIN_RATED_AMOUNT, 0) + NVL (a.PROMO_RATED_AMOUNT, 0) > 0 THEN 1 ELSE 0 END) RATED_TRANSACTION_COUNT,
					  count(distinct CASE WHEN NVL (a.MAIN_RATED_AMOUNT, 0) > 0 THEN SERVED_PARTY END )  MSISDN_RATED_COUNT ,
					  'HOUR' GRANULARITE,
					  current_timestamp INSERT_DATE,
					  TO_DATE(a.TRANSACTION_DATE) TRANSACTION_DATE
            FROM
                MON.FT_VAS_REVENUE_DETAIL  a
            WHERE
                 to_date(a.TRANSACTION_DATE) ='2019-08-07'
                 AND SOURCE_PLATEFORM = 'IN'
            GROUP BY
                 a.TRANSACTION_DATE,
                 a.OTHER_PARTY,
				 cast(HOUR (a.NQ_TRANSACTION_DATE) as string),
				 UPPER (a.COMMERCIAL_PROFILE) ,
				 a.SPECIFIC_TARIFF_INDICATOR,
				 a.SERVICE_CODE ,
				 a.TELESERVICE_INDICATOR,
				 (CASE  WHEN  nvl(a.BILLING_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ),
				 (CASE  WHEN  nvl(a.NETWORK_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ),
				 (CASE WHEN CONCAT(a.LOCATION_MCC,a.LOCATION_MNC) = '62402' THEN LPAD(CAST (CONV (a.LOCATION_LAC ,16,10) AS string), 5, 0) ELSE NULL END)


            UNION ALL

              SELECT
                     b.OTHER_PARTY   VAS_NUMBER,
					 NULL VAS_SHORT_NUMBER,
					 NULL VAS_LONG_NUMBER,
					 'ALL' TRANSACTION_HOUR,
					 UPPER ( b.COMMERCIAL_PROFILE) COMMERCIAL_OFFER,
					 b.SPECIFIC_TARIFF_INDICATOR SPECIFIC_CHARGING_INDICATOR,
					 b.SERVICE_CODE SERVICE_CODE,
					 b.TELESERVICE_INDICATOR,
					 (CASE  WHEN  nvl( b.BILLING_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ) IS_BILLING_TERM_ERROR,
					 (CASE  WHEN  nvl( b.NETWORK_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ) IS_NETWORK_TERM_ERROR,
					 (CASE
                            WHEN CONCAT(b.LOCATION_MCC,b.LOCATION_MNC) = '62402' THEN LPAD (CAST (CONV ( b.LOCATION_LAC, 16,10) AS string), 5, 0)
                            ELSE NULL END )  LOCATION_LAC,
					SUM (1) TRANSACTION_COUNT,
					SUM (CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN NVL ( b.CALL_PROCESS_TOTAL_DURATION, 0) ELSE 0 END) RATED_DURATION,
					SUM (NVL ( b.CALL_PROCESS_TOTAL_DURATION, 0) ) CALL_PROCESS_TOTAL_DURATION,
					SUM ( CEIL  ( (CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN NVL ( b.CALL_PROCESS_TOTAL_DURATION, 0) ELSE 0 END) / 60) ) RATED_DURATION_SPLIT_MINUTE,
					SUM ( CEIL  ( NVL ( b.CALL_PROCESS_TOTAL_DURATION, 0) / 60) )  PROCESS_DURATION_SPLIT_MINUTE,
					SUM (NVL ( b.RATED_VOLUME, 0) ) RATED_VOLUME,
					SUM (NVL ( b.MAIN_REFILL_AMOUNT, 0) ) MAIN_REFILL_AMOUNT,
					SUM (NVL ( b.BUNDLE_REFILL_AMOUNT, 0) ) BUNDLE_REFILL_AMOUNT,
					SUM (NVL ( b.MAIN_RATED_AMOUNT, 0) + NVL ( b.PROMO_RATED_AMOUNT, 0) ) TOTAL_RATED_AMOUNT,
					SUM (NVL ( b.MAIN_RATED_AMOUNT, 0) ) MAIN_RATED_AMOUNT,
					SUM (NVL ( b.PROMO_RATED_AMOUNT, 0) ) PROMO_RATED_AMOUNT,
					SUM (NVL ( b.RATED_AMOUNT_IN_BUNDLE, 0) ) RATED_AMOUNT_IN_BUNDLE,
					SUM (NVL ( b.SMS_USED_VOLUME, 0) ) SMS_USED_VOLUME,
					'IN' SOURCE_PLATEFORM,
					'IN' SOURCE_DATA,
					SUM (CASE WHEN NVL ( b.MAIN_RATED_AMOUNT, 0) + NVL ( b.PROMO_RATED_AMOUNT, 0) > 0 THEN 1 ELSE 0 END) RATED_TRANSACTION_COUNT,
					count(distinct CASE WHEN NVL ( b.MAIN_RATED_AMOUNT, 0) > 0 THEN SERVED_PARTY END )  MSISDN_RATED_COUNT  ,
					'DAY' GRANULARITE,
					CURRENT_TIMESTAMP INSERT_DATE,
					TO_DATE(b.TRANSACTION_DATE) TRANSACTION_DATE
                    FROM
                        MON.FT_VAS_REVENUE_DETAIL  b
                    WHERE
                         to_date(b.TRANSACTION_DATE) = '2019-08-07'
                         AND b.SOURCE_PLATEFORM = 'IN'
                    GROUP BY
                         b.TRANSACTION_DATE,
                         b.OTHER_PARTY,
						 UPPER ( b.COMMERCIAL_PROFILE) ,
						 b.SPECIFIC_TARIFF_INDICATOR,
						 b.SERVICE_CODE ,
						 b.TELESERVICE_INDICATOR,
						 (CASE  WHEN  nvl(b.BILLING_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ),
						 (CASE  WHEN  nvl(b.NETWORK_TERM_INDICATOR,'ND') = '0' THEN  '0'  ELSE '1'  END ),
						 (CASE WHEN CONCAT(b.LOCATION_MCC,b.LOCATION_MNC) = '62402' THEN LPAD(CAST(CONV(b.LOCATION_LAC,16,10) AS string), 5, 0)
                                ELSE NULL END)