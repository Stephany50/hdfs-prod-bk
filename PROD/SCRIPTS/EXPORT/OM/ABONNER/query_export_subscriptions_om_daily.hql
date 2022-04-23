SELECT
    A.MSISDN MSISDN,
    USER_LAST_NAME NOM,
    USER_FIRST_NAME PRENOM,
    ID_NUMBER NUMERO_CNI,
    DOB DATE_NAISSANCE,
    USER_TYPE TYPE_UTIISATEUR,
    ADDRESS,
    SOUSCRIPTION_TYPE,
    --- Conformité
    --Numero téléphone
    (
        CASE 
            WHEN A.MSISDN ='' OR A.MSISDN IS NULL 
            THEN 'OUI' ELSE 'NON' 
        END
    ) MSISDN_ABSENT,
    --(CASE WHEN A.MSISDN =C.CREATED_BY_MSISDN AND A.MSISDN IS NOT NULL THEN 'OUI' ELSE 'NON' END) AS MSISDN_EGAL_CREATED,
        --Numero piéce
    (
        CASE 
            WHEN A.ID_NUMBER = '' OR A.ID_NUMBER IS NULL 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_ABSENT,
    (
        CASE
            WHEN A.DOB > ADD_MONTHS(to_date(current_timestamp()), -21 * 12)  AND A.DOB IS NOT NULL
            THEN 'OUI'
            WHEN A.DOB < ADD_MONTHS(to_date(current_timestamp()), -21 * 12)  AND A.DOB IS NOT NULL
            THEN 'NON'
            WHEN A.DOB IS NULL
            THEN 'NULL'
        END
    ) EST_MINEUR,
    (
        CASE 
            WHEN SUBSTR(A.MSISDN, -9, 9) = A.ID_NUMBER 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_EGALE_MSISDN,
    (
        CASE 
            WHEN LENGTH(A.ID_NUMBER) NOT IN ('9','7','20') 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_TAILLE_DOUTEUSE,
    (
        CASE 
            WHEN LENGTH(TRIM(TRANSLATE(LOWER(A.ID_NUMBER), '0123456789-/',' '))) > 0 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_AVEC_LETTRE,
    (
        CASE 
            WHEN LENGTH(TRIM(TRANSLATE(LOWER(A.ID_NUMBER), 'abcdefghijklmnopqrstuvwxy-/',' '))) = 0 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_UNIQ_LETTRE,
    (
        CASE 
            WHEN LENGTH(TRIM(TRANSLATE(LOWER(A.ID_NUMBER), 'abcdefghijklmnopqrstuvwxyz1234567890-/',' '))) > 0 
            THEN 'OUI' ELSE 'NON' 
        END
    ) NUMERO_PIECE_A_CARACT_NON_AUTH,
        --Nom
    (
        CASE 
            WHEN A.USER_LAST_NAME = '' OR A.USER_LAST_NAME=A.USER_FIRST_NAME OR A.USER_LAST_NAME IS NULL 
            THEN 'OUI' ELSE 'NON'
        END
    ) NOM_ABSENT,
    (
        CASE 
            WHEN LENGTH(TRIM(TRANSLATE(LOWER(A.USER_LAST_NAME),'aeiou',' ')))= 0
                OR LENGTH(TRIM(TRANSLATE(LOWER(A.USER_LAST_NAME),'bcdfghjklmnpqrstvwxz',' ')))= 0
                OR LENGTH(TRIM(TRANSLATE(A.USER_LAST_NAME,'1234567890.',' '))) = 0
                OR LENGTH(A.USER_LAST_NAME) <= 3
                OR LOWER(TRIM(A.USER_LAST_NAME)) IN ('orange','orangemoney','orange money','paul biya','direction general','serveur','central','beac','dg','promo','loto','promotion','tombola', 'unknown','georges', 'pascal', 'marie','antoinette','Gabriel','Léo','Raphaël','Arthur','Louis','Lucas','Adam','Jules','Hugo','Maël','Liam','Noah','Paul','Ethan','Tiago','Sacha','Gabin','Nathan','Mohamed','Aaron','Tom','Éden','Théo','Noé')
                OR LENGTH(TRIM(TRANSLATE(LOWER(A.USER_LAST_NAME),'asdfghjklqwertyuiopzxcvbnm1234567890çéèàäëüïöîôûâê-.''',' ')))> 0 
            THEN 'OUI' ELSE 'NON'
        END
    ) NOM_DOUTEUX,
    --Prenom
    (
        CASE 
            WHEN A.USER_FIRST_NAME = '' OR A.USER_FIRST_NAME=A.USER_LAST_NAME OR A.USER_FIRST_NAME IS NULL 
            THEN 'OUI' ELSE 'NON'
        END
    ) PRENOM_ABSENT,
    (
        CASE WHEN LENGTH(TRIM(TRANSLATE(LOWER(A.USER_FIRST_NAME),'aeiou',' ')))= 0
                OR LENGTH(TRIM(TRANSLATE(LOWER(A.USER_FIRST_NAME),'bcdfghjklmnpqrstvwxz',' ')))= 0
                OR LENGTH(TRIM(TRANSLATE(A.USER_FIRST_NAME,'1234567890',' '))) = 0
                OR LENGTH(A.USER_FIRST_NAME) <= 3
                OR LOWER(TRIM(A.USER_LAST_NAME)) IN ('orange','orangemoney','orange money','paul biya','direction general','serveur','central','beac','dg','promo','loto','promotion','tombola', 'unknown')
                OR LENGTH(TRIM(TRANSLATE(LOWER(A.USER_FIRST_NAME),'asdfghjklqwertyuiopzxcvbnm1234567890çéèàäëüïöîôûâê-.''',' ')))> 0 THEN 'OUI' ELSE 'NON'
        END
    ) PRENOM_DOUTEUX,
        --Année de naissance
    (
        CASE WHEN A.DOB = '' OR A.DOB IS NULL 
        THEN 'OUI' ELSE 'NON' 
        END
    ) DATE_NAISSANCE_ABSENT,
    (
        CASE WHEN A.DOB >= to_date(current_timestamp())  OR A.DOB <= '1940-01-01' 
        THEN 'OUI' ELSE 'NON' 
        END
    ) DATE_NAISSANCE_DOUTEUX,
    (
        CASE 
            WHEN A.ADDRESS = '' OR A.ADDRESS IS NULL 
            THEN 'OUI' ELSE 'NON' 
        END
    ) ADDRESS_ABSENT,
    REGISTERED_ON,
    CREATED_BY,
    CREATED_BY_MSISDN,
    CANAL,
    DISTRIBUTEUR
FROM
(
    SELECT 
        A.MSISDN,
        USER_FIRST_NAME,
        USER_LAST_NAME,
        ADDRESS,
        REGISTERED_ON,
        CREATED_BY,
        CREATED_BY_MSISDN,
        SOUSCRIPTION_TYPE,
        ID_NUMBER,
        DOB,
        USER_TYPE,
        CANAL,
        DISTRIBUTEUR
    FROM 
    (
        select
            MSISDN,
            USER_FIRST_NAME,
            USER_LAST_NAME,
            ADDRESS,
            REGISTERED_ON,
            CREATED_BY,
            CREATED_BY_MSISDN,
            SOUSCRIPTION_TYPE,
            ID_NUMBER,
            USER_TYPE,
            DOB
        from
        (
            SELECT
                MSISDN,
                USER_FIRST_NAME,
                USER_LAST_NAME,
                CONCAT(CONCAT(CONCAT(CONCAT(ADDRESS1,','), ADDRESS2),','), CITY) ADDRESS,
                TO_DATE(REGISTERED_ON) REGISTERED_ON,
                CREATED_BY,
                CREATED_BY_MSISDN,
                SOUSCRIPTION_TYPE,
                ID_NUMBER,
                USER_TYPE,
                TO_DATE(DOB) DOB,
                row_number() over(partition by MSISDN order by modified_on desc) rang
            FROM MON.SPARK_FT_OMNY_USER_REGISTRATION 
            WHERE REGISTERED_ON = '###SLICE_VALUE###' AND TRIM(UPPER(USER_TYPE))='SUBSCRIBER'
        ) A
        where rang = 1
    ) A 
    left join 
    ( 
        select 
            MSISDN,
            CANAL,
            DISTRIBUTEUR 
        from dim.DT_OM_PARTNER_ACCOUNT
    ) Partner  
    on A.CREATED_BY_MSISDN=Partner.MSISDN 
) A

