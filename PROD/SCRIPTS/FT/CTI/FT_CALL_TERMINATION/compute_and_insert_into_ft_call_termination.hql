INSERT INTO   CTI.FT_AGROUP_APPEL PARTITION(CALL_DATE)
SELECT
    IRF.Start_Date_Time_Key,
    FROM_UNIXTIME(IRF.Start_Date_Time_Key+3600) CALL_DATETIME,
    IT.INTERACTION_TYPE_CODE   INTERACTION_TYPE,
    RESOURCE_GROUP.GROUP_NAME  A_GROUP,
    TD.Technical_Result        TECHNICAL_RESULT,
    TD.RESULT_REASON_CODE      RESULT_REASON,
    TD.RESOURCE_ROLE_CODE      RESOURCE_ROLE,
    TD.ROLE_REASON_CODE        ROLE_REASON,
    --UD_CUST_DIM 1
    UDCD1.Dim_Attribute_1      UD_langue,
    UDCD1.Dim_Attribute_2      UD_vipgsm,
    UDCD1.Dim_Attribute_3      UD_vipomy,
    UDCD1.Dim_Attribute_4      UD_isomy,
    UDCD1.Dim_Attribute_5      UD_ListSegment,
    --UD_CUST_DIM 2
    UDCD2.Dim_Attribute_1      UD_segment,
    UDCD2.Dim_Attribute_2      UD_site_cible,
    UDCD2.Dim_Attribute_3      UD_site_choisi,
    UDCD2.Dim_Attribute_4      UD_comp,
    UDCD2.Dim_Attribute_5      UD_comp_deb,
    --UD_CUST_DIM 3
    UDCD3.Dim_Attribute_1      UD_comp_choisi,
    UDCD3.Dim_Attribute_3      UD_crise_site,
    UDCD3.Dim_Attribute_4      UD_crise_ferm,
    UDCD3.Dim_Attribute_5      UD_crise_flux,
    --UD_CUST_DIM 4
    UDCD4.Dim_Attribute_1      UD_crise_dissu,
    UDCD4.Dim_Attribute_2      UD_distributed,
    UDCD4.Dim_Attribute_3      UD_nRONA,
    UDCD4.Dim_Attribute_4      UD_HNO,
    UDCD4.Dim_Attribute_5      UD_Xfer_Presta,
    --UD_CUST_DIM 5
    UDCD5.Dim_Attribute_1      UD_Fermeture,
    UDCD5.Dim_Attribute_2      UD_Dissuasion,
    UDCD5.Dim_Attribute_3      UD_DNIS,
    IRFUD1.Custom_Data_1       UD_DureeAttente,
    IRFUD1.Custom_Data_2       UD_PrioriteFlux,
    --Ce champ sera l'ANI du client
    IRFUD1.Custom_Data_3,
    IRF.Routing_Point_Duration,
    IRF.AFTER_CALL_WORK_DURATION,
    IRF.RING_DURATION,
    IRF.TALK_DURATION,
    IRF.Customer_Talk_Duration,
    IRF.Hold_Duration,
    IRF.Interaction_Id,
    current_timestamp INSERT_DATE,
    to_date(from_unixtime(IRF.Start_Date_Time_Key+3600)) CALL_DATE

FROM
         (SELECT * FROM CTI.IT_INTERACTION_RESOURCE_FACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') IRF
    INNER JOIN (SELECT * FROM  CTI.DT_RESOURCE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.dt_RESOURCE)) RES
    ON   (IRF.Resource_Key=RES.Resource_Key)
     INNER JOIN (SELECT * FROM CTI.DT_TECHNICAL_DESCRIPTOR WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_TECHNICAL_DESCRIPTOR)) TD
    ON   (IRF.Technical_Descriptor_Key=TD.Technical_Descriptor_Key)
    INNER  JOIN (SELECT * FROM CTI.DT_MEDIA_TYPE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_MEDIA_TYPE)) MED
    ON   (IRF.Media_Type_Key=MED.Media_Type_Key)
     INNER JOIN (SELECT * FROM CTI.DT_INTERACTION_TYPE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_INTERACTION_TYPE)) IT
    ON   (IRF.Interaction_Type_Key=IT.Interaction_Type_Key)
     INNER JOIN (SELECT * FROM CTI.FT_IRF_USER_DATA_KEYS WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###')   UDKEY
    ON   (IRF.Interaction_Resource_Id=UDKEY.Interaction_Resource_Id)
     INNER JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_1 WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_USER_DATA_CUST_DIM_1)) UDCD1
    ON   (UDKEY.Custom_Key_1=UDCD1.ID)
     INNER JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_2 WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_USER_DATA_CUST_DIM_2)) UDCD2
    ON   (UDKEY.Custom_Key_2=UDCD2.ID)
     INNER JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_3 WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_USER_DATA_CUST_DIM_3))  UDCD3
    ON   (UDKEY.Custom_Key_3=UDCD3.ID)
     INNER JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_4 WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_USER_DATA_CUST_DIM_4)) UDCD4
    ON   (UDKEY.Custom_Key_4=UDCD4.ID)
     INNER JOIN (SELECT * FROM CTI.DT_USER_DATA_CUST_DIM_5 WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_USER_DATA_CUST_DIM_5)) UDCD5
    ON   (UDKEY.Custom_Key_5=UDCD5.ID)
     INNER JOIN (SELECT * FROM CTI.FT_IRF_USER_DATA_CUST_1 WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###')  IRFUD1
    ON   (IRFUD1.Interaction_Resource_Id=IRF.Interaction_Resource_Id)
     INNER JOIN (SELECT RES.RESOURCE_KEY AS RESOURCE_ID,GR.GROUP_NAME AS GROUP_NAME,RGF.START_TS START_TIME,NVL(RGF.END_TS,'9341423900')END_TIME
            FROM
            (SELECT * FROM CTI.FT_RESOURCE_GROUP_FACT   WHERE ORIGINAL_FILE_DATE = '2012-12-31'  ) RGF
              INNER JOIN (SELECT * FROM CTI.DT_GROUP   WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) GR
              ON   ( RGF.GROUP_KEY=GR.GROUP_KEY)
              INNER JOIN  (SELECT * FROM CTI.DT_RESOURCE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_RESOURCE))  RES
              ON   (RGF.RESOURCE_KEY=RES.RESOURCE_KEY)
             WHERE  RES.RESOURCE_TYPE_CODE='AGENT'
            ) RESOURCE_GROUP
    ON   (RES.RESOURCE_KEY=RESOURCE_GROUP.RESOURCE_ID)
WHERE
     Start_Ts >= RESOURCE_GROUP.START_TIME
 AND IRF.Start_Ts <= RESOURCE_GROUP.END_TIME