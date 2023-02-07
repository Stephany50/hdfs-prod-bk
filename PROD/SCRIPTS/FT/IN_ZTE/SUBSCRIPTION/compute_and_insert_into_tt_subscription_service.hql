insert into tt.subscription_device
SELECT 
    EVENT, 
    MAX(SERVICE_CODE) SERVICE_CODE, 
    MIN(VOIX_ONNET) VOIX_ONNET, 
    MIN(VOIX_OFFNET) VOIX_OFFNET, 
    MIN(VOIX_INTER) VOIX_INTER, 
    MIN(VOIX_ROAMING) VOIX_ROAMING,
    MIN(SMS_ONNET) SMS_ONNET, MIN(SMS_OFFNET) SMS_OFFNET, MIN(SMS_INTER) SMS_INTER, MIN(SMS_ROAMING)SMS_ROAMING, MIN(DATA_BUNDLE) DATA_BUNDLE, MIN(SVA) SVA, MIN(PRIX) PRIX,MIN(combo) COMBO
FROM DIM.DT_SERVICES DTSVS 
GROUP BY EVENT