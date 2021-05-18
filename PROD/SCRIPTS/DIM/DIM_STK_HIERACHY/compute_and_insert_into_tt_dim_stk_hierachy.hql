INSERT INTO TMP.TT_DIM_STK_HIERACHY
SELECT
    C.MSISDN MSISDN,
    C.CATEGORY_CODE CATEGORY_CODE,
    C.CATEGORY_NAME CATEGORY_NAME,
    C.GEOGRAPHICAL_DOMAIN_CODE GEOGRAPHICAL_DOMAIN_CODE,
    C.GEOGRAPHICAL_DOMAIN_NAME GEOGRAPHICAL_DOMAIN_NAME,
    C.CHANNEL_USER_NAME CHANNEL_USER_NAME,
    C.PARENT PARENT,
    C.GRDPARENT GRDPARENT,
    C.ACTIV_BEGIN_DATE ACTIV_BEGIN_DATE,
    C.ACTIV_END_DATE ACTIV_END_DATE,
    C.STATUS STATUS,
    C.LAST_UPDATE LAST_UPDATE,
    (
        CASE
        WHEN C.LAST_EVENT_DATE='###SLICE_VALUE###' THEN NULL
        ELSE  C.TMP_STATUS
        END
    )  TMP_STATUS,
    C.LAST_EVENT_DATE LAST_EVENT_DATE,
    (
        CASE
        WHEN CATEGORY_CODE = 'ORNGPTNR' AND NVL(PARTNER_NAME, 'NOK') = 'NOK'
        THEN SUBSTR(CHANNEL_USER_NAME, (LENGTH(CHANNEL_USER_NAME)+1) - INSTR(REVERSE(CHANNEL_USER_NAME), 'e')+1, LENGTH(CHANNEL_USER_NAME))
        ELSE  C.PARTNER_NAME
        END
    ) PARTNER_NAME,
    C.TDW TDW,
    C.TDW_COCI TDW_COCI,
    C.COMMENTAIRE COMMENTAIRE,
    C.ACTIV_END_TMP ACTIV_END_TMP
FROM
(
SELECT
    A.MSISDN MSISDN,
    A.CATEGORY_CODE CATEGORY_CODE,
    A.CATEGORY_NAME CATEGORY_NAME,
    A.GEOGRAPHICAL_DOMAIN_CODE GEOGRAPHICAL_DOMAIN_CODE,
    A.GEOGRAPHICAL_DOMAIN_NAME GEOGRAPHICAL_DOMAIN_NAME,
    A.CHANNEL_USER_NAME CHANNEL_USER_NAME,
    A.PARENT PARENT,
    A.GRDPARENT GRDPARENT,
    A.ACTIV_BEGIN_DATE ACTIV_BEGIN_DATE,
    (
        CASE
        WHEN NVL(A.TMP_STATUS, 'NOK') <> 'Clos' THEN B.ACTIV_BEGIN_DATE
        ELSE  A.ACTIV_END_DATE
        END
    ) ACTIV_END_DATE,
    (
        CASE
        WHEN NVL(A.TMP_STATUS, 'NOK') <> 'Clos' THEN 'Clos'
        ELSE A.STATUS
        END
    ) STATUS,
    (
        CASE
        WHEN NVL(A.TMP_STATUS, 'NOK') <> 'Clos' THEN CURRENT_DATE()
        ELSE  A.LAST_UPDATE
        END
    ) LAST_UPDATE,
    A.TMP_STATUS TMP_STATUS,
    A.LAST_EVENT_DATE LAST_EVENT_DATE,
    A.PARTNER_NAME PARTNER_NAME,
    A.TDW TDW,
    A.TDW_COCI TDW_COCI,
    (
        CASE
        WHEN NVL(A.TMP_STATUS, 'NOK') <> 'Clos' THEN B.COMMENTAIRE
        ELSE  A.COMMENTAIRE
        END
    ) COMMENTAIRE,
    A.ACTIV_END_TMP ACTIV_END_TMP
FROM
 (
     SELECT
         MSISDN,
         CATEGORY_CODE,
         CATEGORY_NAME,
         GEOGRAPHICAL_DOMAIN_CODE,
         GEOGRAPHICAL_DOMAIN_NAME,
         CHANNEL_USER_NAME,
         PARENT,
         GRDPARENT,
         ACTIV_BEGIN_DATE,
         ACTIV_END_DATE,
         STATUS,
         LAST_UPDATE,
         (
             CASE NVL(STATUS, 'NOK')
             WHEN 'Clox' THEN 'Clos'
             ELSE  TMP_STATUS
             END
         ) TMP_STATUS,
         LAST_EVENT_DATE,
         PARTNER_NAME,
         TDW,
         TDW_COCI,
         COMMENTAIRE,
         ACTIV_END_TMP
         FROM MON.BACKUP_SPARK_DIM_STK_HIERACHY
         WHERE EVENT_DATE='###SLICE_VALUE###'
  ) A
  LEFT JOIN
  (
    SELECT * FROM TMP.TT_FIND_CHANGE_STK
  ) B
  ON A.MSISDN=B.MSISDN
) C