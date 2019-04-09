INSERT INTO MON.FT_X_INTERCO_FINAL PARTITION(SDATE)
SELECT
    SRC
    , CRA_SRC
    , HEURE
    , FAISCEAU
    , USAGE_APPEL
    , NULL INDICATION_APPEL
    , TYPE_APPEL
    , TYPE_ABONNE
    , NULL DESTINATION_APPEL
    ,(CASE
        WHEN CAST(SUBSTRING(HEURE,1,2) AS INT) BETWEEN 7 AND 20 THEN 'HEURE PLEINE'
        ELSE 'HEURE CREUSE'
      END) TYPE_HEURE
    , SUM (NBRE_APPEL) NBRE_APPEL
    , SUM (DUREE_APPEL) DUREE_APPEL
    , CURRENT_TIMESTAMP INSERTED_DATE
    , OPERATOR_CODE
    , SDATE
    FROM
        (

            SELECT
                'FT_AG_INTERCO_FINAL' SRC
                , CRA_SRC
                , SDATE
                , NEW_FAISCEAU FAISCEAU
                , USAGE_APPEL
                , TYPE_APPEL
                , SUM (NVL (NBRE_APPEL, 0)) NBRE_APPEL
                ,SUM(IF(USAGE_APPEL='SMS',0,DUREE_APPEL)) DUREE_APPEL
                , HEURE
                , TYPE_ABONNE
                , OPERATOR_CODE
                FROM
                    ( SELECT

                        FN_FAISEAU_TRUNCK_MSC_HUAWEI (( CASE
                            WHEN USAGE_APPEL = 'Emergency calls' THEN 'Emergency calls'
                            WHEN TRUNCK_OUT = 'CAMTEL' AND PARTNER_ID_LEN = 3 THEN 'Emergency calls NoFlag'
                            ELSE (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                        WHEN 'SMS' THEN 'SMS'
                                        WHEN NULL THEN 'Telephony'
                                       WHEN 'TEL' THEN 'Telephony'
                                        ELSE USAGE_APPEL
                                  END)
                           END ) , type_appel , partner_id, partner_gt, trunck_out, trunck_in, PARTNER_ID_PREFIX, PARTNER_GT_PREFIX, REC_TYPE, SERVICECENTRE) NEW_FAISCEAU
                        , CRA_SRC
                        , SDATE
                        , NVL (DURATION, 0) DUREE_APPEL
                        , CRA_COUNT NBRE_APPEL

                        ,
                        ( CASE
                            WHEN USAGE_APPEL = 'Emergency calls' THEN 'Emergency calls'
                            WHEN TRUNCK_OUT = 'CAMTEL' AND PARTNER_ID_LEN = 3 THEN 'Emergency calls NoFlag'
                            WHEN USAGE_APPEL = 'SMS_A2P' THEN 'SMS_A2P'
                            ELSE
                               (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                        WHEN 'SMS' THEN 'SMS'
                                        WHEN NULL THEN 'Telephony'
                                       WHEN 'TEL' THEN 'Telephony'
                                        ELSE USAGE_APPEL
                                  END)
                          END ) USAGE_APPEL
                        , TYPE_APPEL, HEURE
                        , TYPE_ABONNE
                        , OPERATOR_CODE

                        FROM MON.FT_AG_INTERCO a
                        WHERE

                            SDATE = '###SLICE_VALUE###'
                            AND (CASE
                                    WHEN USAGE_APPEL = 'Emergency calls' THEN 'Emergency calls'
                                    WHEN TRUNCK_OUT = 'CAMTEL' AND PARTNER_ID_LEN = 3 THEN 'Emergency calls NoFlag'
                                    ELSE
                                        (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                            WHEN 'SMS' THEN 'SMS'
                                            WHEN NULL THEN 'Telephony'
                                           WHEN 'TEL' THEN 'Telephony'
                                            ELSE USAGE_APPEL
                                        END)
                                 END)
                             IN ('SMS' , 'Emergency calls', 'Emergency calls NoFlag')
                    ) a
                  WHERE

                      USAGE_APPEL IN ('SMS' , 'Emergency calls', 'Emergency calls NoFlag')
                  GROUP BY
                     CRA_SRC
                   , SDATE
                   , NEW_FAISCEAU
                   , USAGE_APPEL
                   , TYPE_APPEL
                   , HEURE
                   , TYPE_ABONNE
                   , OPERATOR_CODE

            UNION

            SELECT
                'FT_AG_INTERCO_FINAL' SRC , CRA_SRC, SDATE
                , NEW_FAISCEAU FAISCEAU
                , USAGE_APPEL
                , TYPE_APPEL
                , SUM (NVL (NBRE_APPEL, 0)) NBRE_APPEL
                , SUM (IF(USAGE_APPEL='SMS',0,DUREE_APPEL)) DUREE_APPEL
                , HEURE
                , TYPE_ABONNE
                , OPERATOR_CODE
                FROM
                    ( SELECT

                        CRA_SRC
                        , (CASE
                                WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
                                WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
                                WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
                                WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
                                WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'

                                WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                                ELSE 'Orange Cameroun'
                           END ) NEW_FAISCEAU
                        , 'Entrant' TYPE_APPEL
                        , SDATE
                        , NVL (DURATION, 0) DUREE_APPEL
                        , CRA_COUNT NBRE_APPEL
                        ,(CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                WHEN 'SMS' THEN 'SMS'
                                WHEN NULL THEN 'Telephony'
                               WHEN 'TEL' THEN 'Telephony'
                                ELSE USAGE_APPEL
                          END) USAGE_APPEL
                        , HEURE
                        , TYPE_ABONNE
                        , OPERATOR_CODE

                        FROM MON.FT_AG_INTERCO a
                        WHERE

                            SDATE = '###SLICE_VALUE###'

                            AND ( NOT (
                                    (CASE
                                         WHEN TRUNCK_OUT = 'Orange Cameroun' OR SUBSTRING (TRUNCK_OUT, 1, 3) = 'OCM' THEN 'Orange Cameroun'
                                         ELSE TRUNCK_OUT
                                     END) = 'Orange Cameroun'
                                    AND
                                    (CASE
                                        WHEN TRUNCK_IN = 'Orange Cameroun' OR SUBSTRING (TRUNCK_IN, 1, 3) = 'OCM' THEN 'Orange Cameroun'
                                        ELSE TRUNCK_IN
                                    END) = 'Orange Cameroun'
                                     )
                                 )
                    ) a
                  WHERE

                    USAGE_APPEL NOT IN ('SMS' , 'Emergency calls')
                    AND NEW_FAISCEAU <> 'NOT_INTERCONNECT'
                  GROUP BY CRA_SRC, SDATE,  NEW_FAISCEAU, USAGE_APPEL
                   , TYPE_APPEL
                   , HEURE
                   , TYPE_ABONNE
                   , OPERATOR_CODE
            UNION

            SELECT
                'FT_AG_INTERCO_FINAL' SRC , CRA_SRC, SDATE
                , NEW_FAISCEAU FAISCEAU
                , USAGE_APPEL
                , TYPE_APPEL
                , SUM (NVL (NBRE_APPEL, 0)) NBRE_APPEL
                , SUM (IF(USAGE_APPEL='SMS',0,DUREE_APPEL)) DUREE_APPEL
                , HEURE
                , TYPE_ABONNE
                , OPERATOR_CODE
                FROM
                    ( SELECT

                        CRA_SRC
                        , ( CASE
                            WHEN  trunck_out IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_out
                            WHEN  trunck_out = 'CAMTEL' THEN 'Camtel National'
                            WHEN  trunck_out = 'Zain Gabon' THEN  'Zain Gabon'
                            WHEN  trunck_out = 'Zain Tchad' THEN  'Zain Tchad'
                            WHEN  trunck_out = 'Orange CI' THEN  'Orange CI'

                            WHEN  NVL (trunck_out, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                            ELSE 'Orange Cameroun'
                           END ) NEW_FAISCEAU
                        , 'Sortant' TYPE_APPEL
                        , SDATE
                        , NVL (DURATION, 0) DUREE_APPEL
                        , CRA_COUNT NBRE_APPEL

                        ,
                        ( CASE
                            WHEN USAGE_APPEL = 'Emergency calls' THEN 'Emergency calls'
                            WHEN TRUNCK_OUT = 'CAMTEL' AND PARTNER_ID_LEN = 3 THEN 'Emergency calls NoFlag'
                            ELSE
                                (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                        WHEN 'SMS' THEN 'SMS'
                                        WHEN NULL THEN 'Telephony'
                                       WHEN 'TEL' THEN 'Telephony'
                                        ELSE USAGE_APPEL
                                  END)
                         END ) USAGE_APPEL
                        , HEURE
                        , TYPE_ABONNE
                        , OPERATOR_CODE

                        FROM MON.FT_AG_INTERCO a
                        WHERE

                            SDATE = '###SLICE_VALUE###'

                            AND ( NOT (
                                (CASE WHEN TRUNCK_OUT = 'Orange Cameroun' OR SUBSTRING (TRUNCK_OUT, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_OUT END ) = 'Orange Cameroun'
                                AND (CASE WHEN TRUNCK_IN = 'Orange Cameroun' OR SUBSTRING (TRUNCK_IN, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_IN END ) = 'Orange Cameroun'
                                ) )
                    ) a
                  WHERE

                    USAGE_APPEL NOT IN ('SMS' , 'Emergency calls', 'Emergency calls NoFlag')
                    AND NEW_FAISCEAU <> 'NOT_INTERCONNECT'
                  GROUP BY CRA_SRC, SDATE,  NEW_FAISCEAU, USAGE_APPEL
                   , TYPE_APPEL
                   , HEURE
                   , TYPE_ABONNE
                   , OPERATOR_CODE
            UNION

            SELECT
                'FT_AG_INTERCO_FINAL' SRC
                , CRA_SRC
                , SDATE
                , NEW_FAISCEAU FAISCEAU
                , USAGE_APPEL
                , TYPE_APPEL
                , SUM (NVL (NBRE_APPEL, 0)) NBRE_APPEL
                , SUM (IF(USAGE_APPEL='SMS',0,DUREE_APPEL)) DUREE_APPEL
                , HEURE
                , TYPE_ABONNE
                , OPERATOR_CODE
                FROM
                    ( SELECT

                        CRA_SRC
                        , ( CASE
                            WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
                            WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
                            WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
                            WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
                            WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'

                            WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                            ELSE 'Orange Cameroun'
                           END ) NEW_FAISCEAU
                        , 'Entrant' TYPE_APPEL
                        , SDATE
                        , NVL (DURATION, 0) DUREE_APPEL
                        , CRA_COUNT NBRE_APPEL
                        , (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                WHEN 'SMS' THEN 'SMS'
                                WHEN NULL THEN 'Telephony'
                               WHEN 'TEL' THEN 'Telephony'
                                ELSE USAGE_APPEL
                          END) USAGE_APPEL
                        , HEURE
                        , TYPE_ABONNE
                        , OPERATOR_CODE

                        FROM MON.FT_AG_INTERCO a
                        WHERE

                            SDATE = '###SLICE_VALUE###'

                            AND (
                                (CASE WHEN TRUNCK_OUT = 'Orange Cameroun' OR SUBSTRING (TRUNCK_OUT, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_OUT END ) = 'Orange Cameroun'
                                AND (CASE WHEN TRUNCK_IN = 'Orange Cameroun' OR SUBSTRING (TRUNCK_IN, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_IN END ) = 'Orange Cameroun'
                                )

                            AND rec_type = 1
                    ) a
                  WHERE

                    USAGE_APPEL NOT IN ('SMS' , 'Emergency calls')
                    AND NEW_FAISCEAU <> 'NOT_INTERCONNECT'
                  GROUP BY CRA_SRC, SDATE,  NEW_FAISCEAU, USAGE_APPEL
                   , TYPE_APPEL
                   , HEURE
                   , TYPE_ABONNE
                   , OPERATOR_CODE
            UNION

            SELECT
                'FT_AG_INTERCO_FINAL' SRC , CRA_SRC, SDATE
                , NEW_FAISCEAU FAISCEAU
                , USAGE_APPEL
                , TYPE_APPEL
                , SUM (NVL (NBRE_APPEL, 0)) NBRE_APPEL
                , SUM (IF(USAGE_APPEL='SMS',0,DUREE_APPEL)) DUREE_APPEL
                , HEURE
                , TYPE_ABONNE
                , OPERATOR_CODE
                FROM
                    ( SELECT

                        CRA_SRC
                        , ( CASE
                            WHEN  trunck_out IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_out
                            WHEN  trunck_out = 'CAMTEL' THEN 'Camtel National'
                            WHEN  trunck_out = 'Zain Gabon' THEN  'Zain Gabon'
                            WHEN  trunck_out = 'Zain Tchad' THEN  'Zain Tchad'
                            WHEN  trunck_out = 'Orange CI' THEN  'Orange CI'

                            WHEN  NVL (trunck_out, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                            ELSE 'Orange Cameroun'
                           END ) NEW_FAISCEAU
                        , 'Sortant' TYPE_APPEL
                        , SDATE
                        , NVL (DURATION, 0) DUREE_APPEL
                        , CRA_COUNT NBRE_APPEL
                        , (CASE SUBSTRING (USAGE_APPEL, 1, 3)
                                WHEN 'SMS' THEN 'SMS'
                                WHEN NULL THEN 'Telephony'
                               WHEN 'TEL' THEN 'Telephony'
                                ELSE USAGE_APPEL
                          END) USAGE_APPEL
                        , HEURE
                        , TYPE_ABONNE
                        , OPERATOR_CODE

                        FROM MON.FT_AG_INTERCO a
                        WHERE

                            SDATE = '###SLICE_VALUE###'

                            AND (
                                (CASE WHEN TRUNCK_OUT = 'Orange Cameroun' OR SUBSTRING (TRUNCK_OUT, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_OUT END ) = 'Orange Cameroun'
                                AND (CASE WHEN TRUNCK_IN = 'Orange Cameroun' OR SUBSTRING (TRUNCK_IN, 1, 3) = 'OCM' THEN 'Orange Cameroun' ELSE TRUNCK_IN END ) = 'Orange Cameroun'
                                )

                            AND rec_type = 0
                    ) a
                  WHERE

                    USAGE_APPEL NOT IN ('SMS' , 'Emergency calls')
                    AND NEW_FAISCEAU <> 'NOT_INTERCONNECT'
                  GROUP BY
                     CRA_SRC
                   , SDATE
                   ,  NEW_FAISCEAU
                   , USAGE_APPEL
                   , TYPE_APPEL
                   , HEURE
                   , TYPE_ABONNE
                   , OPERATOR_CODE
        )T1

        GROUP BY
              SRC
            , CRA_SRC
            , SDATE
            , HEURE
            , FAISCEAU
            , USAGE_APPEL
            , TYPE_APPEL
            , type_abonne
            , (CASE
                    WHEN CAST(SUBSTRING(HEURE,1,2) AS INT) BETWEEN 7 AND 20 THEN 'HEURE PLEINE'
                    ELSE 'HEURE CREUSE'
               END)
            , OPERATOR_CODE

;
