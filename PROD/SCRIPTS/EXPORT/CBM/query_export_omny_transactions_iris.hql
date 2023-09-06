SELECT
    SENDER_MSISDN ,
    RECEIVER_MSISDN,
    RECEIVER_USER_ID,
    SENDER_USER_ID,
    TRANSACTION_AMOUNT,
    COMMISSIONS_PAID,
    COMMISSIONS_RECEIVED,
    COMMISSIONS_OTHERS,
    SERVICE_CHARGE_RECEIVED,
    SERVICE_CHARGE_PAID,
    TAXES,
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
    ) SERVICE_TYPE,
    TRANSFER_STATUS,
    SENDER_PRE_BAL,
    SENDER_POST_BAL,
    RECEIVER_PRE_BAL,
    RECEIVER_POST_BAL,
    SENDER_ACC_STATUS,
    RECEIVER_ACC_STATUS,
    ERROR_CODE,
    ERROR_DESC,
    REFERENCE_NUMBER,
    CREATED_ON,
    CREATED_BY,
    MODIFIED_ON,
    MODIFIED_BY,
    APP_1_DATE,
    APP_2_DATE,
    TRANSFER_ID,
    TRANSFER_DATETIME,
    SENDER_CATEGORY_CODE,
    SENDER_DOMAIN_CODE,
    SENDER_GRADE_NAME,
    SENDER_GROUP_ROLE,
    SENDER_DESIGNATION,
    SENDER_STATE,
    RECEIVER_CATEGORY_CODE,
    RECEIVER_DOMAIN_CODE,
    RECEIVER_GRADE_NAME,
    RECEIVER_GROUP_ROLE,
    RECEIVER_DESIGNATION,
    RECEIVER_STATE,
    SENDER_CITY,
    RECEIVER_CITY,
    APP_1_BY,
    APP_2_BY,
    REQUEST_SOURCE,
    GATEWAY_TYPE,
    TRANSFER_SUBTYPE,
    PAYMENT_TYPE,
    PAYMENT_NUMBER,
    PAYMENT_DATE,
    REMARKS,
    ACTION_TYPE,
    TRANSACTION_TAG,
    RECONCILIATION_BY,
    RECONCILIATION_FOR,
    EXT_TXN_NUMBER,
    ORIGINAL_REF_NUMBER,
    ZEBRA_AMBIGUOUS,
    ATTEMPT_STATUS,
    OTHER_MSISDN,
    SENDER_WALLET_NUMBER,
    RECEIVER_WALLET_NUMBER,
    SENDER_USER_NAME,
    RECEIVER_USER_NAME,
    TNO_MSISDN,
    TNO_ID ,
    UNREG_FIRST_NAME,
    UNREG_LAST_NAME,
    UNREG_DOB,
    UNREG_ID_NUMBER,
    BULK_PAYOUT_BATCHID,
    IS_FINANCIAL ,
    TRANSFER_DONE,
    INITIATOR_MSISDN,
    VALIDATOR_MSISDN,
    INITIATOR_COMMENTS,
    VALIDATOR_COMMENTS,
    SENDER_WALLET_NAME,
    RECIEVER_WALLET_NAME,
    SENDER_USER_TYPE,
    RECEIVER_USER_TYPE
FROM (select * from CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE FILE_DATE = '###SLICE_VALUE###') T0
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
from DIM.REF_OM_PRODUCTS where ref_date<='###SLICE_VALUE###') 
and upper(technology) like "%WEB%" or upper(technology) like "%FLAS%") Q
on receiver_msisdn= Q.msisdn
left join
(select distinct msisdn from DIM.REF_OM_PRODUCTS where ref_date in (select max(ref_date) 
from DIM.REF_OM_PRODUCTS 
where ref_date<='###SLICE_VALUE###') and upper(product) like "%FOND%") R
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