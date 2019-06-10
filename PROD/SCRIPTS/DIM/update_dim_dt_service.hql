MERGE INTO DIM.DT_SERVICES A
USING (
    SELECT IPP_NAME,IPP_CODE,MIN(IPP_AMOUNT) IPP_AMOUNT,MIN(CREATED_DATE)
    FROM CDR.IT_IPP_EXTRACT
    WHERE ORIGINAL_FILE_DATE = (select max(original_file_date)
                                       from
                                  (select original_file_date from cdr.it_ipp_extract
                                 where original_file_date <= d_slice_value
                                  group by original_file_date))
    group by IPP_NAME,IPP_CODE
    ) B
    ON (A.EVENT = B.IPP_NAME)
   WHEN MATCHED THEN
       UPDATE SET A.PRIX = B.IPP_AMOUNT
   WHEN NOT MATCHED THEN
   INSERT(A.EVENT,A.PRIX)VALUES(B.IPP_NAME,B.IPP_AMOUNT);


INSERT INTO TF_SUBSCRIPTION_02
 SELECT  TRANSACTION_DATE, TRANSACTION_TIME, SERVED_PARTY_MSISDN, CONTRACT_TYPE, COMMERCIAL_OFFER, OPERATOR_CODE, SUBSCRIPTION_CHANNEL, SERVICE_LIST, SUBSCRIPTION_SERVICE
         , SUBSCRIPTION_SERVICE_DETAILS, SUBSCRIPTION_RELATED_SERVICE,
         (Case when SUBSCRIPTION_CHANNEL='32' then Serv.prix else RATED_AMOUNT end) RATED_AMOUNT,
          MAIN_BALANCE_USED, ACTIVE_DATE, ACTIVE_TIME, EXPIRE_DATE, EXPIRE_TIME
         , SUBSCRIPTION_STATUS, PREVIOUS_COMMERCIAL_OFFER, PREVIOUS_STATUS, PREVIOUS_SUBS_SERVICE_DETAILS, PREVIOUS_SUBS_RELATED_SERVICE, TERMINATION_INDICATOR
         , BENEFIT_BALANCE_LIST, BENEFIT_UNIT_LIST, BENEFIT_ADDED_VALUE_LIST, BENEFIT_RESULT_VALUE_LIST, BENEFIT_ACTIVE_DATE_LIST, BENEFIT_EXPIRE_DATE_LIST, TOTAL_OCCURENCE
         , INSERT_DATE, SOURCE_INSERT_DATE, ORIGINAL_FILE_NAME, a.SERVICE_CODE
    ,  (Rated_Amount * nvl(voix_onnet, 0)) voix_onnet
    , (Rated_Amount * nvl(voix_offnet, 0))  voix_offnet
    ,  (Rated_Amount * nvl(voix_inter, 0))  voix_inter
    ,( Rated_Amount * nvl(voix_roaming, 0)) voix_roaming
    , (Rated_Amount * nvl(sms_onnet, 0))  sms_onnet
    , ( Rated_Amount * nvl(sms_offnet, 0)) sms_offnet
    , ( Rated_Amount * nvl(sms_inter, 0)) sms_inter
    ,( Rated_Amount * nvl(sms_roaming, 0))  sms_roaming
    , ( Rated_Amount * nvl(data_bundle, 0))  data_bundle
    ,( Rated_Amount * nvl(sva, 0))   sva,
    (CASE WHEN SUBSCRIPTION_CHANNEL='32' THEN  Serv.prix ELSE 0 END) Amount_via_OM,
    (CASE WHEN SUBSCRIPTION_CHANNEL='31' THEN Serv.prix ELSE 0 END) Amount_via_VAS
    FROM TF_SUBSCRIPTION a
    , (SELECT EVENT, min(SERVICE_CODE) SERVICE_CODE, min(voix_onnet) Voix_onnet, min(voix_offnet) voix_offnet, min(voix_inter)voix_inter
        , min(voix_roaming)voix_roaming, min(sms_onnet) sms_onnet, min(sms_offnet) sms_offnet, min(sms_inter) sms_inter, min(sms_roaming)sms_roaming
        , min(data_bundle) data_bundle, min(sva) sva, min(prix) prix
        FROM Dim.dt_Services
        GROUP BY EVENT ) Serv
    where a.subscription_service_details = event(+);
 Zone de message


 Envoyer un message à Jean Paul