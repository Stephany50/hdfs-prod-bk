INSERT INTO MON.FT_CREDIT_TRANSFER PARTITION(REFILL_DATE)
SELECT
 TRANSFER_ID
 ,REFILL_TIME
 ,SENDER_MSISDN
 ,COMMERCIAL_OFFER
 ,RECEIVER_IMSI
 ,RECEIVER_MSISDN
 ,TRANSFER_VOLUME
 ,TRANSFER_VOLUME_UNIT
 ,SENDER_DEBIT_AMT
 ,TRANSFER_AMT
 ,TRANSFER_FEES
 ,TRANSFER_MEAN
 ,TERMINATION_IND
 ,(CASE
	 WHEN sender_msisdn_operator='OCM' THEN
		(CASE
			WHEN LENGTH (sender_msisdn_formatted) = 9 AND SUBSTR(sender_msisdn_formatted,1,3) = '692' THEN 'SET'
			WHEN LENGTH (sender_msisdn_formatted) = 8 AND SUBSTR(sender_msisdn_formatted,1,2) = '92' THEN 'SET'
			ELSE 'OCM'
		 END)
	 ELSE 'UNKNOWN_OPERATOR'
  END) SENDER_OPERATOR_CODE
 ,(CASE
	 WHEN receiver_msisdn_operator='OCM' THEN
		(CASE
			WHEN LENGTH (receiver_msisdn_formatted) = 9 AND SUBSTR(receiver_msisdn_formatted,1,3) = '692' THEN 'SET'
			WHEN LENGTH (receiver_msisdn_formatted) = 8 AND SUBSTR(receiver_msisdn_formatted,1,2) = '92' THEN 'SET'
			ELSE 'OCM'
		 END)
	 ELSE 'UNKNOWN_OPERATOR'
  END) RECEIVER_OPERATOR_CODE
 ,MAX(ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME
 ,INSERT_DATE
 ,REFILL_DATE
FROM (
  SELECT
    a.COMMAND_NAME TRANSFER_ID
    , DATE_FORMAT(a.START_DATE_TIME,'HHmmss') REFILL_TIME
    , a.MSISDN_SRC SENDER_MSISDN
    , NVL(b.PROFILE_NAME, a.PROFILE_SRC) COMMERCIAL_OFFER
    , ' ' RECEIVER_IMSI
    , a.MSISDN_DEST   RECEIVER_MSISDN
    , a.AMOUNT TRANSFER_VOLUME
    , 'CFA' TRANSFER_VOLUME_UNIT
    , a.TOTAL_AMOUNT_DEBIT  SENDER_DEBIT_AMT
    , a.AMOUNT TRANSFER_AMT
    , a.TRANSACTION_COST TRANSFER_FEES
    , 'P2P' TRANSFER_MEAN
    , a.RETURN_CODE TERMINATION_IND
    , (CASE
	      WHEN LENGTH (sender_msisdn_formatted) = 8 THEN
		    (CASE
			    WHEN (substr(sender_msisdn_formatted,1,1) = '9') OR (substr(sender_msisdn_formatted,1,2) IN ('55', '56', '57', '58', '59')) THEN 'OCM'
			    WHEN (substr(sender_msisdn_formatted,1,1) = '7') OR (substr(sender_msisdn_formatted,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83'))  THEN 'MTN'
			    WHEN substr(sender_msisdn_formatted,1,1) = '6' OR substr(sender_msisdn_formatted,1,2) = '85'  THEN 'VIETTEL'
			    WHEN substr(sender_msisdn_formatted,1,1) in ('2','3') THEN 'CAMTEL'
			    ELSE 'INTERNATIONAL_CMR'
		    END)
	      WHEN LENGTH (sender_msisdn_formatted) = 9 THEN
		    (CASE
			    WHEN (substr(sender_msisdn_formatted,1,2) = '69') OR (substr(sender_msisdn_formatted,1,3) IN ('655', '656', '657', '658', '659')) THEN 'OCM'
			    WHEN (substr(sender_msisdn_formatted,1,2) = '67') OR (substr(sender_msisdn_formatted,1,3) IN ('650', '651', '652', '653', '654','680','681','682','683')) THEN 'MTN'
			    WHEN substr(sender_msisdn_formatted,1,2) = '66' or substr(sender_msisdn_formatted,1,3) = '685'  THEN 'VIETTEL'
			    WHEN substr(sender_msisdn_formatted,1,3) in ('243','242','222','233') THEN 'CAMTEL' 
			    WHEN substr(sender_msisdn_formatted,1,2) = '62' THEN 'CAMTEL_MOB'
			    ELSE 'INTERNATIONAL_CMR'
		    END)      
	      WHEN LENGTH (sender_msisdn_formatted) = 13 AND substr(sender_msisdn_formatted,1,3)= '160' THEN
		    (CASE
			    WHEN (substr(sender_msisdn_formatted,1,4) = '1602') THEN 'OCM'
		  	  WHEN (substr(sender_msisdn_formatted,1,4) = '1601') THEN 'MTN'
		  	  WHEN (substr(sender_msisdn_formatted,1,4) = '1603')  THEN 'VIETTEL'
			    ELSE 'INTERNATIONAL_CMR'
		    END)       
	      WHEN LENGTH (sender_msisdn_formatted) > 9 THEN 'INTERNATIONAL'
	      ELSE 'OCM_SHORT'
      END) sender_msisdn_operator
    , (CASE
	      WHEN LENGTH (receiver_msisdn_formatted) = 8 THEN
		    (CASE
			    WHEN (substr(receiver_msisdn_formatted,1,1) = '9') OR (substr(receiver_msisdn_formatted,1,2) IN ('55', '56', '57', '58', '59')) THEN 'OCM'
			    WHEN (substr(receiver_msisdn_formatted,1,1) = '7') OR (substr(receiver_msisdn_formatted,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83'))  THEN 'MTN'
			    WHEN substr(receiver_msisdn_formatted,1,1) = '6' OR substr(receiver_msisdn_formatted,1,2) = '85'  THEN 'VIETTEL'
			    WHEN substr(receiver_msisdn_formatted,1,1) in ('2','3') THEN 'CAMTEL'
			    ELSE 'INTERNATIONAL_CMR'
		    END)
	      WHEN LENGTH (receiver_msisdn_formatted) = 9 THEN
		    (CASE
			    WHEN (substr(receiver_msisdn_formatted,1,2) = '69') OR (substr(receiver_msisdn_formatted,1,3) IN ('655', '656', '657', '658', '659')) THEN 'OCM'
			    WHEN (substr(receiver_msisdn_formatted,1,2) = '67') OR (substr(receiver_msisdn_formatted,1,3) IN ('650', '651', '652', '653', '654','680','681','682','683')) THEN 'MTN'
			    WHEN substr(receiver_msisdn_formatted,1,2) = '66' or substr(receiver_msisdn_formatted,1,3) = '685'  THEN 'VIETTEL'
			    WHEN substr(receiver_msisdn_formatted,1,3) in ('243','242','222','233') THEN 'CAMTEL' 
			    WHEN substr(receiver_msisdn_formatted,1,2) = '62' THEN 'CAMTEL_MOB'
			    ELSE 'INTERNATIONAL_CMR'
		    END)      
	      WHEN LENGTH (receiver_msisdn_formatted) = 13 AND substr(receiver_msisdn_formatted,1,3)= '160' THEN
		    (CASE
			    WHEN (substr(receiver_msisdn_formatted,1,4) = '1602') THEN 'OCM'
			    WHEN (substr(receiver_msisdn_formatted,1,4) = '1601') THEN 'MTN'
			    WHEN (substr(receiver_msisdn_formatted,1,4) = '1603')  THEN 'VIETTEL'
			    ELSE 'INTERNATIONAL_CMR'
		    END)       
	      WHEN LENGTH (receiver_msisdn_formatted) > 9 THEN 'INTERNATIONAL'
	      ELSE 'OCM_SHORT'
      END) receiver_msisdn_operator
    , sender_msisdn_formatted
    , receiver_msisdn_formatted
    , a.ORIGINAL_FILE_NAME ORIGINAL_FILE_NAME
    , CURRENT_TIMESTAMP INSERT_DATE
    , TO_DATE(a.START_DATE_TIME) REFILL_DATE
  FROM 
  (SELECT *
  , (CASE
	    WHEN MSISDN_SRC IN ('44534952454D494E4445', '534D5350415243') 
		  THEN '99999999' 
	    WHEN LENGTH(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC)) = 9 
	    AND  SUBSTR(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC), 1, 1) NOT IN ('0', '2') 
		  THEN NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC)
	    WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC),"^0+(?!$)",""), 1, 3) = '237' 
	    AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC),"^0+(?!$)","")) > 3 
		  THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC),"^0+(?!$)",""), 4)
	    ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_SRC,'[0-9]+', 0),MSISDN_SRC),"^0+(?!$)",""))  
    END) sender_msisdn_formatted
, (CASE
	  WHEN MSISDN_DEST IN ('44534952454D494E4445', '534D5350415243') 
		THEN '99999999' 
	  WHEN LENGTH(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST)) = 9 
	  AND  SUBSTR(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST), 1, 1) NOT IN ('0', '2') 
		THEN NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST)
	  WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST),"^0+(?!$)",""), 1, 3) = '237' 
	  AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST),"^0+(?!$)","")) > 3 
		THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST),"^0+(?!$)",""), 4)
	  ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN_DEST,'[0-9]+', 0),MSISDN_DEST),"^0+(?!$)",""))  
  END) receiver_msisdn_formatted
FROM CDR.IT_P2P_LOG) a
LEFT JOIN ( 
  SELECT PROFILE_NAME, STD_CODE, MAX(ORIGINAL_FILE_DATE) 
  FROM CDR.SPARK_IT_ZTE_PROFILE
  WHERE ORIGINAL_FILE_DATE <= '###SLICE_VALUE###' 
  GROUP BY PROFILE_NAME, STD_CODE) b 
ON a.PROFILE_SRC = b.STD_CODE
WHERE a.START_DATE = '###SLICE_VALUE###' AND SUBSTR(TRIM(a.USSD_ORDER),1,3)='TRA'
) T_RESULT
GROUP BY 
TRANSFER_ID
 ,REFILL_TIME
 ,SENDER_MSISDN
 ,COMMERCIAL_OFFER
 ,RECEIVER_IMSI
 ,RECEIVER_MSISDN
 ,TRANSFER_VOLUME
 ,TRANSFER_VOLUME_UNIT
 ,SENDER_DEBIT_AMT
 ,TRANSFER_AMT
 ,TRANSFER_FEES
 ,TRANSFER_MEAN
 ,TERMINATION_IND
 ,(CASE
	 WHEN sender_msisdn_operator='OCM' THEN
		(CASE
			WHEN LENGTH (sender_msisdn_formatted) = 9 AND SUBSTR(sender_msisdn_formatted,1,3) = '692' THEN 'SET'
			WHEN LENGTH (sender_msisdn_formatted) = 8 AND SUBSTR(sender_msisdn_formatted,1,2) = '92' THEN 'SET'
			ELSE 'OCM'
		 END)
	 ELSE 'UNKNOWN_OPERATOR'
  END)
 ,(CASE
	 WHEN receiver_msisdn_operator='OCM' THEN
		(CASE
			WHEN LENGTH (receiver_msisdn_formatted) = 9 AND SUBSTR(receiver_msisdn_formatted,1,3) = '692' THEN 'SET'
			WHEN LENGTH (receiver_msisdn_formatted) = 8 AND SUBSTR(receiver_msisdn_formatted,1,2) = '92' THEN 'SET'
			ELSE 'OCM'
		 END)
	 ELSE 'UNKNOWN_OPERATOR'
  END)
 ,INSERT_DATE
 ,REFILL_DATE
;
