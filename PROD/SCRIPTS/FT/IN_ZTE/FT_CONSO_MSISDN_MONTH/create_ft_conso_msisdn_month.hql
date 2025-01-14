CREATE TABLE MON.FT_CONSO_MSISDN_DAY
(
  MSISDN                          VARCHAR(25),
  FORMULE                         VARCHAR(70),
  CONSO                           DECIMAL(17,2),
  SMS_COUNT                       DECIMAL(17,2),
  TEL_COUNT                       DECIMAL(17,2),
  TEL_DURATION                    DECIMAL(17,2),
  BILLED_SMS_COUNT                DECIMAL(17,2),
  BILLED_TEL_COUNT                DECIMAL(17,2),
  BILLED_TEL_DURATION             DECIMAL(17,2),
  CONSO_SMS                       DECIMAL(17,2),
  CONSO_TEL                       DECIMAL(17,2),
  PROMOTIONAL_CALL_COST           DECIMAL(17,2),
  MAIN_CALL_COST                  DECIMAL(17,2),
  SRC_TABLE                       VARCHAR(30),
  OTHERS_VAS_TOTAL_COUNT          DECIMAL(17,2),
  OTHERS_VAS_DURATION             DECIMAL(17,2),
  OTHERS_VAS_MAIN_COST            DECIMAL(17,2),
  OTHERS_VAS_PROMO_COST           DECIMAL(17,2),
  NATIONAL_TOTAL_COUNT            DECIMAL(17,2),
  NATIONAL_SMS_COUNT              DECIMAL(17,2),
  NATIONAL_DURATION               DECIMAL(17,2),
  NATIONAL_MAIN_COST              DECIMAL(17,2),
  NATIONAL_PROMO_COST             DECIMAL(17,2),
  NATIONAL_SMS_MAIN_COST          DECIMAL(17,2),
  NATIONAL_SMS_PROMO_COST         DECIMAL(17,2),
  MTN_TOTAL_COUNT                 DECIMAL(17,2),
  MTN_SMS_COUNT                   DECIMAL(17,2),
  MTN_DURATION                    DECIMAL(17,2),
  MTN_TOTAL_CONSO                 DECIMAL(17,2),
  MTN_SMS_CONSO                   DECIMAL(17,2),
  CAMTEL_TOTAL_COUNT              DECIMAL(17,2),
  CAMTEL_SMS_COUNT                DECIMAL(17,2),
  CAMTEL_DURATION                 DECIMAL(17,2),
  CAMTEL_TOTAL_CONSO              DECIMAL(17,2),
  CAMTEL_SMS_CONSO                DECIMAL(17,2),
  INTERNATIONAL_TOTAL_COUNT       DECIMAL(17,2),
  INTERNATIONAL_SMS_COUNT         DECIMAL(17,2),
  INTERNATIONAL_DURATION          DECIMAL(17,2),
  INTERNATIONAL_TOTAL_CONSO       DECIMAL(17,2),
  INTERNATIONAL_SMS_CONSO         DECIMAL(17,2),
  ONNET_SMS_COUNT                 DECIMAL(17,2),
  ONNET_DURATION                  DECIMAL(17,2),
  ONNET_TOTAL_CONSO               DECIMAL(17,2),
  ONNET_MAIN_CONSO                DECIMAL(17,2),
  ONNET_MAIN_TEL_CONSO            DECIMAL(17,2),
  ONNET_PROMO_TEL_CONSO           DECIMAL(17,2),
  ONNET_SMS_CONSO                 DECIMAL(17,2),
  MTN_MAIN_CONSO                  DECIMAL(17,2),
  CAMTEL_MAIN_CONSO               DECIMAL(17,2),
  SET_TOTAL_COUNT                 DECIMAL(17,2),
  SET_SMS_COUNT                   DECIMAL(17,2),
  SET_DURATION                    DECIMAL(17,2),
  SET_TOTAL_CONSO                 DECIMAL(17,2),
  SET_SMS_CONSO                   DECIMAL(17,2),
  SET_MAIN_CONSO                  DECIMAL(17,2),
  INTERNATIONAL_MAIN_CONSO        DECIMAL(17,2),
  ROAM_TOTAL_COUNT                DECIMAL(17,2),
  ROAM_SMS_COUNT                  DECIMAL(17,2),
  ROAM_DURATION                   DECIMAL(17,2),
  ROAM_TOTAL_CONSO                DECIMAL(17,2),
  ROAM_MAIN_CONSO                 DECIMAL(17,2),
  ROAM_SMS_CONSO                  DECIMAL(17,2),
  INROAM_TOTAL_COUNT              DECIMAL(17,2),
  INROAM_SMS_COUNT                DECIMAL(17,2),
  INROAM_DURATION                 DECIMAL(17,2),
  INROAM_TOTAL_CONSO              DECIMAL(17,2),
  INROAM_MAIN_CONSO               DECIMAL(17,2),
  INROAM_SMS_CONSO                DECIMAL(17,2),
  OPERATOR_CODE                   VARCHAR(50),
  NEXTTEL_TOTAL_COUNT             DECIMAL(17,2),
  NEXTTEL_SMS_COUNT               DECIMAL(17,2),
  NEXTTEL_DURATION                DECIMAL(17,2),
  NEXTTEL_TOTAL_CONSO             DECIMAL(17,2),
  NEXTTEL_SMS_CONSO               DECIMAL(17,2),
  NEXTTEL_MAIN_CONSO              DECIMAL(17,2),
  BUNDLE_SMS_COUNT                DECIMAL(17,2),
  BUNDLE_TEL_DURATION             DECIMAL(17,2),
  SET_MAIN_TEL_CONSO              DECIMAL(17,2),
  MTN_MAIN_TEL_CONSO              DECIMAL(17,2),
  NEXTTEL_MAIN_TEL_CONSO          DECIMAL(17,2),
  CAMTEL_MAIN_TEL_CONSO           DECIMAL(17,2),
  INTERNATIONAL_MAIN_TEL_CONSO    DECIMAL(17,2),
  SET_PROMO_TEL_CONSO             DECIMAL(17,2),
  MTN_PROMO_TEL_CONSO             DECIMAL(17,2),
  NEXTTEL_PROMO_TEL_CONSO         DECIMAL(17,2),
  CAMTEL_PROMO_TEL_CONSO          DECIMAL(17,2),
  INTERNATIONAL_PROMO_TEL_CONSO   DECIMAL(17,2),
  ROAM_MAIN_TEL_CONSO             DECIMAL(17,2),
  INROAM_MAIN_TEL_CONSO           DECIMAL(17,2),
  ROAM_PROMO_TEL_CONSO            DECIMAL(17,2),
  INROAM_PROMO_TEL_CONSO          DECIMAL(17,2),
  ONNET_BILLED_TEL_DURATION       DECIMAL(17,2),
  SET_BILLED_TEL_DURATION         DECIMAL(17,2),
  MTN_BILLED_TEL_DURATION         DECIMAL(17,2),
  NEXTTEL_BILLED_TEL_DURATION     DECIMAL(17,2),
  CAMTEL_BILLED_TEL_DURATION      DECIMAL(17,2),
  INTERNATIONAL_BIL_TEL_DURATION  DECIMAL(17,2),
  ROAM_BILLED_TEL_DURATION        DECIMAL(17,2),
  INROAM_BILLED_TEL_DURATION      DECIMAL(17,2),
  ONNET_BILLED_TEL_COUNT          DECIMAL(17,2),
  SET_BILLED_TEL_COUNT            DECIMAL(17,2),
  MTN_BILLED_TEL_COUNT            DECIMAL(17,2),
  NEXTTEL_BILLED_TEL_COUNT        DECIMAL(17,2),
  CAMTEL_BILLED_TEL_COUNT         DECIMAL(17,2),
  INTERNATIONAL_BILLED_TEL_COUNT  DECIMAL(17,2),
  ROAM_BILLED_TEL_COUNT           DECIMAL(17,2),
  INROAM_BILLED_TEL_COUNT         DECIMAL(17,2),
  ONNET_TEL_COUNT                 DECIMAL(17,2),
  SET_TEL_COUNT                   DECIMAL(17,2),
  MTN_TEL_COUNT                   DECIMAL(17,2),
  NEXTTEL_TEL_COUNT               DECIMAL(17,2),
  CAMTEL_TEL_COUNT                DECIMAL(17,2),
  INTERNATIONAL_TEL_COUNT         DECIMAL(17,2),
  ROAM_TEL_COUNT                  DECIMAL(17,2),
  INROAM_TEL_COUNT                DECIMAL(17,2),
  CONSO_TEL_MAIN                  DECIMAL(17,2),
  SVA_COUNT                       DECIMAL(17,2),
  SVA_DURATION                    DECIMAL(17,2),
  SVA_MAIN_CONSO                  DECIMAL(17,2),
  SVA_PROMO_CONSO                 DECIMAL(17,2),
  SVA_TEL_COUNT                   DECIMAL(17,2),
  SVA_BILLED_DURATION             DECIMAL(17,2),
  SVA_BILLED_TEL_CONSO            DECIMAL(17,2),
  SVA_SMS_COUNT                   DECIMAL(17,2),
  SVA_SMS_CONSO                   DECIMAL(17,2),
  INSERT_DATE                     TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")