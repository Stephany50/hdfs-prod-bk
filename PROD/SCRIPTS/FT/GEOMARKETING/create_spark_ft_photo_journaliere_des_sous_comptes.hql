CREATE TABLE MON.SPARK_FT_PHOTO_JOURNALIERE_DES_SOUS_COMPTES
(
    msisdn varchar(100)                   --
    , bal_id bigint                       -- identifiant du sous compte
    , transaction_time timestamp          -- date et heure où l'abonné effectue la souscription
    , acct_res_id int                     -- informations supplementaires sur le sous compte
    , acct_res_rating_unit varchar(10) -- connaitre l'unite de mesure du sous compte
    , politic varchar(20)                 -- politique de gestion du sous compte : cumul, ecrase, instance
    , bdle_name varchar(100)              -- forfait en cours dans la balance consideree
    , prix_total DECIMAL(30, 8)           -- prix total initial du bundle
    , volume_total DECIMAL(30, 8)         -- volume total initial du bundle (part affectee au sous compte)
    , duree_bundle bigint                 -- duree de validite du forfait
    , initial_date timestamp              -- date de debut du forfait dans le sous compte
    , expiration_date timestamp           -- date de fin du forfait dans le sous compte
    , cumul_usage DECIMAL(30, 8)          -- cumul des usages effectués dans ce sous compte à la date j
    , volume_restant DECIMAL(30, 8)          -- volume restant dans le sous compte
    , cumul_valorisation DECIMAL(30, 8)   -- cumul des valorisations des usages effectués dans ce sous compte à la date j-1
    , valorisation_restante DECIMAL(30, 8) -- prix_total-cumul_valorisation
    , nb_jours_ecoules int                -- utile pour calculer l'usage final prevu
    , insert_date timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
