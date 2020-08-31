INSERT INTO MON.spark_ft_om_apgl_transaction PARTITION(DOCUMENT_DATE)
                SELECT
                    ACCOUNTING_TYPE,
                    ACCOUNT_NO,
                    POSTING_DATE,
                    DOCUMENT_NO,
                    DESCRIPTION,
                    TRANSACTION_LINE_DESCRIPTION,
                    CURRENCY_CODE,
                    AMOUNT,
                    AMOUNT_LVC,
                    SOUS_COMPTE,
                    ORGANISATION,
                    OFFRE,
                    PARTNER,
                    SITE,
                    PROJECT,
                    FLUX,
                    SC8,
                    SALESPERSON_CODE,
                    DUE_DATE,
                    PAYMENT_DISCOUNT,
                    PAYMENT_TERMS_CODE,
                    EXTERNAL_DOC_NO,
                   CURRENT_TIMESTAMP INSERT_DATE,
                    DOCUMENT_DATE
                FROM
                (

                        SELECT
                            ACCOUNTING_TYPE,
                            (CASE
                                WHEN ACCOUNT_NO = '47713000' THEN '44313000'
                                WHEN ACCOUNT_NO = '63222000' THEN '44521000'
                                WHEN ACCOUNT_NO = '70711000' THEN '44313000'
                                ELSE ACCOUNT_NO
                            END) ACCOUNT_NO,
                            POSTING_DATE,
                            DOCUMENT_NO,
                            DESCRIPTION,
                            TRANSACTION_LINE_DESCRIPTION,
                            CURRENCY_CODE,
                            ROUND(AMOUNT - AMOUNT / 1.1925) AMOUNT,
                            ROUND(AMOUNT_LVC - AMOUNT_LVC / 1.1925) AMOUNT_LVC,
                            NULL SOUS_COMPTE,
                            NULL ORGANISATION,
                            NULL OFFRE,
                            NULL PARTNER,
                            NULL SITE,
                            NULL PROJECT,
                            '10' FLUX,
                            NULL SC8,
                            NULL SALESPERSON_CODE,
                            NULL DUE_DATE,
                            NULL PAYMENT_DISCOUNT,
                            NULL PAYMENT_TERMS_CODE,
                            NULL EXTERNAL_DOC_NO,
                            ROWNUMBER,
                            DOCUMENT_DATE
                        FROM TMP.SPARK_TT_OM_APGL_TRANSACTION
                        WHERE ACCOUNT_NO IN ('47713000', '63222000', '70711000')


                    UNION ALL

                        SELECT
                            ACCOUNTING_TYPE,
                            ACCOUNT_NO,
                            POSTING_DATE,
                            DOCUMENT_NO,
                            DESCRIPTION,
                            TRANSACTION_LINE_DESCRIPTION,
                            CURRENCY_CODE,
                            (CASE
                                WHEN ACCOUNT_NO IN ('47713000', '63222000', '70711000') THEN ROUND(AMOUNT / 1.1925)
                                ELSE ROUND(AMOUNT)
                            END) AMOUNT,
                            (CASE
                                WHEN ACCOUNT_NO IN ('47713000', '63222000', '70711000') THEN ROUND(AMOUNT_LVC / 1.1925)
                                ELSE ROUND(AMOUNT_LVC)
                            END) AMOUNT_LVC,
                            NULL SOUS_COMPTE,
                            NULL ORGANISATION,
                            NULL OFFRE,
                            NULL PARTNER,
                            NULL SITE,
                            NULL PROJECT,
                            FLUX,
                            NULL SC8,
                            NULL SALESPERSON_CODE,
                            NULL DUE_DATE,
                            NULL PAYMENT_DISCOUNT,
                            NULL PAYMENT_TERMS_CODE,
                            NULL EXTERNAL_DOC_NO,
                            ROWNUMBER,
                           DOCUMENT_DATE
                        FROM TMP.SPARK_TT_OM_APGL_TRANSACTION



                ) P ORDER BY ROWNUMBER