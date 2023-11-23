SELECT
    T0.SENDER_MSISDN ,
    T0.RECEIVER_MSISDN,
    T0.RECEIVER_USER_ID,
    T0.SENDER_USER_ID,
    T0.TRANSACTION_AMOUNT,
    T0.COMMISSIONS_PAID,
    T0.COMMISSIONS_RECEIVED,
    T0.COMMISSIONS_OTHERS,
    T0.SERVICE_CHARGE_RECEIVED,
    T0.SERVICE_CHARGE_PAID,
    T0.TAXES,
    (
        case when transfer_status='TS' and substr(transfer_id,1,2)='PP' and
            sender_category_code='SUBS' and A.msisdn is null and B.msisdn is null then "P2P_OUT_OM"

        when transfer_status='TS' and  substr(transfer_id,1,2)='PN' then "TNO_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CO' and C.msisdn is null and D.msisdn is null then "CASHOUT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CN' then "CASHOUT_OFFNET_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='PP' and sender_category_code<>'SUBS' 
            and receiver_category_code<>'SUBS' then "FEE_B2B_TRANSACTION_OM"
        
        when transfer_status='TS' and substr(transfer_id,1,2)='CI' and F.msisdn is not null
            then "B2W_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CO' and E.msisdn is not null
            then "W2B_OM"

        when transfer_status='TS' and substr(transfer_id,1,2) in ('BP','MP','IN') and G.msisdn is not null and B.msisdn is null
            then "ENEO_OM"
        
        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and I.msisdn is not null and B.msisdn is null then "CANAL_PLUS_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and receiver_msisdn='657763384' and B.msisdn is null then "STARTIME_OM" 

        when transfer_status='TS' and substr(transfer_id,1,2) in ('BP','MP','IN') and K.msisdn is not null and B.msisdn is null
            then "CAMWATER_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='BP' and receiver_msisdn not in ('699703939','AES')
                and L.msisdn is null and B.msisdn is null then "AUTRE_FACTURES_OM" 

        when transfer_status='TS' and ((substr(transfer_id,1,2)='PP' and upper(receiver_grade_name) like "%INFORMEL%" and 
                A.msisdn is null) or substr(transfer_id,1,2)='MP' and 
                receiver_msisdn<>'AES' and N.msisdn is null and B.msisdn is null and 
                O.msisdn is null) then "MERCHPAY_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='SM' and receiver_msisdn <>'AES' and 
                N.msisdn is null and B.msisdn is null then "OM_ASSO_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and sender_category_code='SUBS' and Q.msisdn is not null
                and N.msisdn is null and P.msisdn is null 
                then "WEB_PAIEMENT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='ER' then "PAIEMENT_MASSE_OM"

        when transfer_status='TS' and  substr(transfer_id,1,2)='MP' and sender_category_code='SUBS' and R.msisdn is not null 
                and N.msisdn is null and P.msisdn is null
                then "REMONTEE_FOND_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='RC' then"TOP_UP_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and S.msisdn is not null
                and B.msisdn is null then "DUAL_PAIMENT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CI' and receiver_category_code='SUBS' 
            and C.msisdn is null and D.msisdn is null then "CASHIN_OM"

        when transfer_status='TS' and substr(transfer_id,1,2) in ('CO','MP') and T.msisdn is not null
                then "VISA_OM"

        when transfer_status='TS' and U.msisdn is not null and B.msisdn is null and 
            substr(transfer_id,1,2) in ('CI','CO','PN','RC','MP','BP','ER')  then "MICRO_ASSURANCE_OM"

        when transfer_status='TS' and service_type="CASHIN" and sender_category_code<>'SUBS' and 
                V.msisdn is not null then "GIMAC_IN_OM"

        when transfer_status='TS' and service_type in ('CASHOUT', 'P2P', 'MERCHPAY') and sender_category_code='SUBS' and 
                (W.msisdn is not null and X.msisdn is null) then "GIMACT_OUT_OM"

        when transfer_status='TS' and sender_category_code='IRTCAT' and sender_msisdn in ("691400678","692037846",
            "692037849","692049000") and A.msisdn is null then "IRT_OM"

        else service_type end
    ) service_type,
    T0.TRANSFER_STATUS,
    T0.SENDER_PRE_BAL,
    T0.SENDER_POST_BAL,
    T0.RECEIVER_PRE_BAL,
    T0.RECEIVER_POST_BAL,
    T0.SENDER_ACC_STATUS,
    T0.RECEIVER_ACC_STATUS,
    T0.ERROR_CODE,
    T0.ERROR_DESC,
    T0.REFERENCE_NUMBER,
    T0.CREATED_ON,
    T0.CREATED_BY,
    T0.MODIFIED_ON,
    T0.MODIFIED_BY,
    T0.APP_1_DATE,
    T0.APP_2_DATE,
    T0.TRANSFER_ID,
    T0.transfer_datetime_nq as transfer_datetime,
    T0.SENDER_CATEGORY_CODE,
    T0.SENDER_DOMAIN_CODE,
    T0.SENDER_GRADE_NAME,
    T0.SENDER_GROUP_ROLE,
    T0.SENDER_DESIGNATION,
    T0.SENDER_STATE,
    T0.RECEIVER_CATEGORY_CODE,
    T0.RECEIVER_DOMAIN_CODE,
    T0.RECEIVER_GRADE_NAME,
    T0.RECEIVER_GROUP_ROLE,
    T0.RECEIVER_DESIGNATION,
    T0.RECEIVER_STATE,
    T0.SENDER_CITY,
    T0.RECEIVER_CITY,
    T0.APP_1_BY,
    T0.APP_2_BY,
    T0.REQUEST_SOURCE,
    T0.GATEWAY_TYPE,
    T0.TRANSFER_SUBTYPE,
    T0.PAYMENT_TYPE,
    T0.PAYMENT_NUMBER,
    T0.PAYMENT_DATE,
    T0.REMARKS,
    T0.ACTION_TYPE,
    T0.TRANSACTION_TAG,
    T0.RECONCILIATION_BY,
    T0.RECONCILIATION_FOR,
    T0.EXT_TXN_NUMBER,
    T0.ORIGINAL_REF_NUMBER,
    T0.ZEBRA_AMBIGUOUS,
    T0.ATTEMPT_STATUS,
    T0.OTHER_MSISDN,
    T0.SENDER_WALLET_NUMBER,
    T0.RECEIVER_WALLET_NUMBER,
    T0.SENDER_USER_NAME,
    T0.RECEIVER_USER_NAME,
    T0.TNO_MSISDN,
    T0.TNO_ID ,
    T0.UNREG_FIRST_NAME,
    T0.UNREG_LAST_NAME,
    T0.UNREG_DOB,
    T0.UNREG_ID_NUMBER,
    T0.BULK_PAYOUT_BATCHID,
    T0.IS_FINANCIAL ,
    T0.TRANSFER_DONE,
    T0.INITIATOR_MSISDN,
    T0.VALIDATOR_MSISDN,
    T0.INITIATOR_COMMENTS,
    T0.VALIDATOR_COMMENTS,
    T0.SENDER_WALLET_NAME,
    T0.RECIEVER_WALLET_NAME,
    T0.SENDER_USER_TYPE,
    T0.RECEIVER_USER_TYPE
FROM (select * from CDR.SPARK_IT_OMNY_TRANSACTIONS_HOURLY WHERE file_time = '###SLICE_VALUE###') T0
left join 
(select msisdn from DIM.OM_TEST_MSISDNS) A on receiver_msisdn= A.msisdn
left join 
(select msisdn from DIM.OM_TEST_MSISDNS) B on sender_msisdn= B.msisdn
left join 
(select msisdn from DIM.OM_TEST_MSISDNS union select msisdn from DIM.msisdn_VISA 
union select msisdn from DIM.msisdn_B2W union select msisdn from DIM.msisdn_GIMAC) C
on receiver_msisdn= C.msisdn
left join
(select msisdn from DIM.OM_TEST_MSISDNS union select msisdn from DIM.msisdn_VISA 
union select msisdn from DIM.msisdn_B2W union select msisdn from DIM.msisdn_GIMAC) D
on sender_msisdn= D.msisdn
left join
(select msisdn from DIM.msisdn_B2W) E on receiver_msisdn= E.msisdn
left join
(select msisdn from DIM.msisdn_B2W) F on sender_msisdn= F.msisdn
left join
(select msisdn from DIM.msisdn_ENEO) G on receiver_msisdn= G.msisdn
left join
(select msisdn from DIM.msisdn_ENEO) H on sender_msisdn= H.msisdn
left join
(select msisdn from DIM.msisdn_CANAL) I on receiver_msisdn= I.msisdn
left join 
(select msisdn from DIM.msisdn_EAU) K on receiver_msisdn= K.msisdn
left join 
(select msisdn from DIM.msisdn_EAU union select msisdn from DIM.ENEOPREPAID_MSISDNS
union select msisdn from DIM.OM_TEST_MSISDNS) L on receiver_msisdn= L.msisdn
left join 
(select msisdn from DIM.NON_MERCHANT_MSISDNS union select msisdn from DIM.OM_TEST_MSISDNS) N
on receiver_msisdn= N.msisdn
left join 
(select msisdn from DIM.msisdn_EAU union select msisdn from DIM.REF_OM_PRODUCTS 
where upper(technology) like "%WEB" or upper(technology) like "%FLAS%") O
on receiver_msisdn= O.msisdn
left join 
(select msisdn from DIM.NON_MERCHANT_MSISDNS union select msisdn from DIM.OM_TEST_MSISDNS) P
on sender_msisdn= P.msisdn
left join
(select distinct msisdn from DIM.REF_OM_PRODUCTS where ref_date in (select max(ref_date) 
from DIM.REF_OM_PRODUCTS where ref_date<='2023-11-14') 
and upper(technology) like "%WEB%" or upper(technology) like "%FLAS%") Q
on receiver_msisdn= Q.msisdn
left join
(select distinct msisdn from DIM.REF_OM_PRODUCTS where ref_date in (select max(ref_date) 
from DIM.REF_OM_PRODUCTS 
where ref_date<='2023-11-14') and upper(product) like "%FOND%") R
on receiver_msisdn= R.msisdn
left join 
(select msisdn from DIM.INTERNET_MSISDN_1) S
on receiver_msisdn= S.msisdn
left join
(select msisdn from DIM.msisdn_VISA) T
on receiver_msisdn= T.msisdn
left join
(select msisdn from DIM.MICROASSURANCE_MSISDNS) U
on receiver_msisdn= U.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC union select msisdn from DIM.msisdn_GIMAC_REF) V
on sender_msisdn= V.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC) W
on receiver_msisdn= W.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC_REF) X
on receiver_msisdn= X.msisdn