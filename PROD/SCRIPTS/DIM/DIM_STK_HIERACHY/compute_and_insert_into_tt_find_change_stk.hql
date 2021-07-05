INSERT INTO  TMP.TT_FIND_CHANGE_STK
SELECT
    B.*,
    NULL,
    NULL,
    A.TDW,
    A.TDW_COCI,
    (
        CASE WHEN NVL(A.CATEGORY_CODE, 'Unknwon') <> NVL(b.category_code, 'Unknwon') THEN 'Category Code Modify'
            WHEN NVL(A.PARENT, 'Unknwon') <> NVL(B.PARENT, 'Unknwon') THEN 'Parent Modify'
            WHEN NVL(A.GRDPARENT, 'Unknwon') <> NVL(B.GRDPARENT, 'Unknwon') THEN'Grand Parent Modify'
            WHEN NVL(A.CHANNEL_USER_NAME, 'Unknwon') <> NVL(B.CHANNEL_USER_NAME, 'Unknwon') THEN 'Channel User Name Modify'
        END
    ) COMMENTAIRE
FROM
(
    SELECT
        *
    FROM MON.BACKUP_SPARK_DIM_STK_HIERACHY
    WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###', 1)
        AND NVL(STATUS, 'NOK') <> 'Clos'
) A
RIGHT JOIN
TMP.TT_STK_HIERACHY B
ON A.MSISDN=B.MSISDN
WHERE  NVL(A.CATEGORY_CODE, 'Unknwon') <> NVL(B.CATEGORY_CODE, 'Unknwon')
        OR NVL(A.PARENT, 'Unknwon') <> NVL(B.PARENT, 'Unknwon')
        OR NVL(A.GRDPARENT, 'Unknwon') <> NVL(B.GRDPARENT, 'Unknwon')
        OR NVL(A.CHANNEL_USER_NAME, 'Unknwon') <> NVL(B.CHANNEL_USER_NAME, 'Unknwon')