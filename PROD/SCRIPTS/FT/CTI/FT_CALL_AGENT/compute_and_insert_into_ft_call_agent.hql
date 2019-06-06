
INSERT INTO CTI.FT_CALL_AGENT PARTITION(SDATE)
SELECT

    IRF.START_DATE_TIME_KEY,
    IT.INTERACTION_TYPE_CODE          INTERACTION_TYPE,
    RES.RESOURCE_NAME                 RESOURCE_NAME,
    RES.AGENT_FIRST_NAME              AGENT_FIRST_NAME,
    RES.AGENT_LAST_NAME               AGENT_LAST_NAME,
    RES.RESOURCE_TYPE_CODE            RESOURCE_TYPE,

    TD.TECHNICAL_RESULT               TECHNICAL_RESULT,
    TD.RESULT_REASON_CODE             RESULT_REASON,
    TD.RESOURCE_ROLE_CODE             RESOURCE_ROLE,
    TD.ROLE_REASON_CODE               ROLE_REASON,

    UDCD1.DIM_ATTRIBUTE_1             UD_LANGUE,
    UDCD1.DIM_ATTRIBUTE_2             UD_VIPGSM,
    UDCD1.DIM_ATTRIBUTE_3             UD_VIPOMY,
    UDCD1.DIM_ATTRIBUTE_4             UD_ISOMY,
    UDCD1.DIM_ATTRIBUTE_5             UD_LISTSEGMENT,

    UDCD2.DIM_ATTRIBUTE_1             UD_SEGMENT,
    UDCD2.DIM_ATTRIBUTE_2             UD_SITE_CIBLE,
    UDCD2.DIM_ATTRIBUTE_3             UD_SITE_CHOISI,
    UDCD2.DIM_ATTRIBUTE_4             UD_COMP,
    UDCD2.DIM_ATTRIBUTE_5             UD_COMP_DEB,

    UDCD3.DIM_ATTRIBUTE_1             UD_COMP_CHOISI,
    UDCD3.DIM_ATTRIBUTE_3             UD_CRISE_SITE,
    UDCD3.DIM_ATTRIBUTE_4             UD_CRISE_FERM,
    UDCD3.DIM_ATTRIBUTE_5             UD_CRISE_FLUX,

    UDCD4.DIM_ATTRIBUTE_1             UD_CRISE_DISSU,
    UDCD4.DIM_ATTRIBUTE_2             UD_DISTRIBUTED,
    UDCD4.DIM_ATTRIBUTE_3             UD_NRONA,
    UDCD4.DIM_ATTRIBUTE_4             UD_HNO,
    UDCD4.DIM_ATTRIBUTE_5             UD_XFER_PRESTA,

    UDCD5.DIM_ATTRIBUTE_1             UD_FERMETURE,
    UDCD5.DIM_ATTRIBUTE_2             UD_DISSUASION,
    UDCD5.DIM_ATTRIBUTE_3             UD_DNIS,
    IRFUD1.CUSTOM_DATA_1              UD_DUREEATTENTE,
    IRFUD1.CUSTOM_DATA_2              UD_PRIORITEFLUX,

    IRFUD1.CUSTOM_DATA_3 CLIENT,
    IRF.ROUTING_POINT_DURATION   TPS_RP,
    IRF.AFTER_CALL_WORK_DURATION TPS_ACW,
    IRF.RING_DURATION          TPS_SONNERIE,
    IRF.TALK_DURATION          TPS_CONVERSATION_AGENT,
    IRF.CUSTOMER_TALK_DURATION TPS_CONVERSATION_CLIENT,
    IRF.HOLD_DURATION          HOLD_DURATION,
    IRF.INTERACTION_ID         RF_INTERACTION_ID,
    FROM_UTC_TIMESTAMP(CAST(IRF.START_DATE_TIME_KEY AS BIGINT) * 1000, '${hivevar:time_zone}') SDATE_DATETIME,
    current_timestamp INSERT_DATE,
    TO_DATE(FROM_UTC_TIMESTAMP(CAST(IRF.START_DATE_TIME_KEY AS BIGINT) * 1000, '${hivevar:time_zone}')) SDATE

FROM

    (SELECT * FROM CTI.IT_INTERACTION_RESOURCE_FACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') IRF
        JOIN (SELECT * FROM CTI.DT_RESOURCE WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_RESOURCE)) RES ON (RES.RESOURCE_KEY=IRF.RESOURCE_KEY)
        JOIN (SELECT * FROM CTI.DT_TECHNICAL_DESCRIPTOR WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_TECHNICAL_DESCRIPTOR)) TD ON (TD.TECHNICAL_DESCRIPTOR_KEY=IRF.TECHNICAL_DESCRIPTOR_KEY)
        JOIN (SELECT * FROM CTI.DT_MEDIA_TYPE WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_MEDIA_TYPE)) MED ON (MED.MEDIA_TYPE_KEY=IRF.MEDIA_TYPE_KEY)
        JOIN (SELECT * FROM CTI.DT_INTERACTION_TYPE WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_INTERACTION_TYPE)) IT ON (IRF.INTERACTION_TYPE_KEY=IT.INTERACTION_TYPE_KEY)
        JOIN (SELECT * FROM CTI.FT_IRF_USER_DATA_KEYS WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') UDKEY ON (UDKEY.INTERACTION_RESOURCE_ID=IRF.INTERACTION_RESOURCE_ID)
        JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_1 WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_USER_DATA_CUST_DIM_1)) UDCD1 ON (UDKEY.CUSTOM_KEY_1=UDCD1.ID)
        JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_2 WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_USER_DATA_CUST_DIM_2)) UDCD2 ON (UDKEY.CUSTOM_KEY_2=UDCD2.ID)
        JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_3 WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_USER_DATA_CUST_DIM_3)) UDCD3 ON (UDKEY.CUSTOM_KEY_3=UDCD3.ID)
        JOIN(SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_4 WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_USER_DATA_CUST_DIM_4)) UDCD4 ON (UDKEY.CUSTOM_KEY_4=UDCD4.ID)
        JOIN(SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_5 WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) from CTI.DT_USER_DATA_CUST_DIM_5)) UDCD5 ON (UDKEY.CUSTOM_KEY_5=UDCD5.ID)
        JOIN(SELECT * FROM CTI.FT_IRF_USER_DATA_CUST_1 WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') IRFUD1 ON (IRFUD1.INTERACTION_RESOURCE_ID=IRF.INTERACTION_RESOURCE_ID)