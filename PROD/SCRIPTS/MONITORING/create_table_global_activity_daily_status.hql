Bonsoir Stépahne,

Ci-dessous le contenu du sprint planning #8 . Nous avons jusqu’à 182 points pour ce sprint et proposons d’étendre le sprint à  4 semaines.

===========================================
Le sprint de Octobre : Sprint#8
===========================================
--------------------------------------------------------------------------------------------------------------
Sprint Goal: Integration de BSCS et Vue 360
--------------------------------------------------------------------------------------------------------------
Sprint duration: 3 weeks (09/10 - 29/10)
Sprint Review: (30/10)
Sprint Retrospective: (31/10)
Totals user stories points: (182)

Sprint Items:


- VUE CLIENT 360 (DATAMART_MARKETING , BDI et AUTRES)
    -- implementation de client 360
                              * Table temp msisdn unique (M)
                              * Ajout des infos de base (IN,MSC,OM,MVAS,DATA,SUBS) (XL)
                              * Ajout des nouvelles colonnes. (L)
                                            SMSC, ED,BASE_I,SUBS,OM_IT_TRANSAC, ACCOUNT_SNAP_OM, ...
    -- qualification (L)
    -- Resortir la FT_MARKETING_DATAMART (L)
    -- Resortir la FT_BDI (L)

- BSCS(BALANCE_AGEE , BDI ET CONTRACT_SNAPSHOT)
               -- Preparer la transition avec Zsmart (L)
                              * Comprend le fonctionnement de Zsmart
                              * Resortir l''ensemble des extracts qui repondent à notre besoin
               -- Collecte des extracts BSCS (L)

               -- Bascule et Calcul de la BDI
                              * Conception de la collecte des données BSCS (L)
                                                           **Soit Executer la requete sur le serveur distant via Nifi
                                                           **Soit Importer les tables dependantes
                                                                                                                      customer_all
                                                                                                                      , info_cust_text
                                                                                                                      , contract_all
                                                                                                                      , info_contr_text
                                                                                                                      , contr_services_cap
                                                                                                                      , directory_number
                                                                                                                      , ccontact_all
                                                                                                                      , curr_co_status

                                                                                                                      )
                              * Comprendre et implementer la logic derrière la derogation (ALCATEL.IDENTIFICATION_DEROGATION) (L)
                              * Conception de la collecte des données  HLR (S)
                                            * OK_MemExpFile_HSS9860_1200_(\\d+)_:date:(\\d{6}).tar.gz
                              * Implementation (L)
                                            * Croisement des données BSCS et HLR (recuperer le statut des services c-a-d appel_entrant, appel_sortant du msisdn) ()
                                            * Validation et qualification


- Nettoyage du Cluster
               -- Mise en production de la purge des ITs (S)
               -- Alert sur les tables FTs mal calculees (S)
               -- Corriger les FTs sur les 3 dernier mois (L)
               -- Notification Cronmanager (S)
               -- Notification flux individuel de l IN (S)
               -- Purge des repertoires & logs (M)


- OM
               --Calcul IT Transaction (S)

- SMS Spool
                              * MISSING_FILES (XS)
                              * RIA,CBM,OM  (L)

- LOCALISATION SUR l''IN (M)
               -- Ajout des champs (SUBSCRIPTION, VOICE_SMS)


- Stabilisation de la chaine CTI streaming (M)


- CONSOLIDATION DE LA CHAINE
    -- calcul de IT et FT (regularité des données)
           * toutes les tables RIA (M)
           * toutes les tables OM (respect de la nommenclature OMNY) (M)




CREATE TABLE IF NOT EXISTS MON.GLOBAL_ACTIVITY_DAILY_STATUS2(
    SOURCE_DATA STRING,
    TAXED_AMOUNT BIGINT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
