SELECT
  APPROX_COUNT_DISTINCT(ORIGINAL_FILE_DATE) DISTINCT_ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE
   FROM in_zte_recharge_tmp3