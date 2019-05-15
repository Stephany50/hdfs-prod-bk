INSERT INTO CTI.IT_INTERACTION_RESOURCE_FACT
SELECT
    INTERACTION_RESOURCE_ID              ,
    TENANT_KEY                              ,
    INTERACTION_TYPE_KEY                    ,
    MEDIA_TYPE_KEY                          ,
    TECHNICAL_DESCRIPTOR_KEY                ,
    MEDIA_RESOURCE_KEY                      ,
    RESOURCE_GROUP_COMBINATION_KEY          ,
    PLACE_KEY                               ,
    STRATEGY_KEY                            ,
    ROUTING_TARGET_KEY                      ,
    REQUESTED_SKILL_KEY                     ,
    INTERACTION_ID                        ,
    RES_PREVIOUS_SM_STATE_KEY               ,
    RES_PREVIOUS_SM_STATE_FACT_KEY        ,
    RESOURCE_KEY                            ,
    LAST_RP_RESOURCE_KEY                    ,
    LAST_QUEUE_RESOURCE_KEY                 ,
    LAST_VQUEUE_RESOURCE_KEY                ,
    LAST_IVR_RESOURCE_KEY                   ,
    PREV_IRF_ID                           ,
    MEDIATION_SEGMENT_ID                   ,
    MEDIATION_RESOURCE_KEY                  ,
    MEDIATION_START_DATE_TIME_KEY          ,
    INTERACTION_RESOURCE_ORDINAL         ,
    IRF_ANCHOR                               ,
    IRF_ANCHOR_DATE_TIME_KEY               ,
    LAST_INTERACTION_RESOURCE                ,
    PARTYGUID                            ,
    LEAD_CLIP_DURATION                      ,
    TRAIL_CLIP_DURATION                    ,
    ROUTING_POINT_DURATION                  ,
    QUEUE_DURATION                          ,
    IVR_PORT_DURATION                       ,
    HANDLE_COUNT                             ,
    CUSTOMER_HANDLE_COUNT                   ,
    PREVIOUS_MEDIATION_DURATION           ,
    MEDIATION_DURATION                      ,
    MEDIATION_COUNT                          ,
    MET_SERVICE_OBJECTIVE_FLAG               ,
    SHORT_ABANDONED_FLAG                     ,
    STOP_ACTION                          ,
    DIAL_COUNT                           ,
    DIAL_DURATION                          ,
    RING_COUNT                               ,
    RING_DURATION                           ,
    TALK_COUNT                              ,
    TALK_DURATION                           ,
    HOLD_COUNT                               ,
    HOLD_DURATION                           ,
    AFTER_CALL_WORK_COUNT                    ,
    AFTER_CALL_WORK_DURATION                ,
    CUSTOMER_DIAL_COUNT                      ,
    CUSTOMER_DIAL_DURATION                  ,
    CUSTOMER_RING_COUNT                      ,
    CUSTOMER_RING_DURATION                  ,
    CUSTOMER_TALK_COUNT                      ,
    CUSTOMER_TALK_DURATION                  ,
    CUSTOMER_HOLD_COUNT                      ,
    CUSTOMER_HOLD_DURATION                  ,
    CUSTOMER_ACW_COUNT                       ,
    CUSTOMER_ACW_DURATION                   ,
    POST_CONS_XFER_TALK_COUNT                ,
    POST_CONS_XFER_TALK_DURATION            ,
    POST_CONS_XFER_HOLD_COUNT                ,
    POST_CONS_XFER_HOLD_DURATION           ,
    POST_CONS_XFER_RING_COUNT               ,
    POST_CONS_XFER_RING_DURATION            ,
    CONF_INIT_TALK_COUNT               ,
    CONF_INIT_TALK_DURATION                ,
    CONF_INIT_HOLD_COUNT                  ,
    CONF_INIT_HOLD_DURATION                 ,
    CONF_JOIN_RING_COUNT                    ,
    CONF_JOIN_RING_DURATION                ,
    CONF_JOIN_TALK_COUNT                     ,
    CONF_JOIN_TALK_DURATION               ,
    CONF_JOIN_HOLD_COUNT                    ,
    CONF_JOIN_HOLD_DURATION                 ,
    CONFERENCE_INITIATED_COUNT              ,
    CONS_INIT_DIAL_COUNT                  ,
    CONS_INIT_DIAL_DURATION              ,
    CONS_INIT_TALK_COUNT                    ,
    CONS_INIT_TALK_DURATION ,
    CONS_INIT_HOLD_COUNT  ,
    CONS_INIT_HOLD_DURATION                ,
    CONS_RCV_RING_COUNT                     ,
    CONS_RCV_RING_DURATION           ,
    CONS_RCV_TALK_COUNT                   ,
    CONS_RCV_TALK_DURATION                 ,
    CONS_RCV_HOLD_COUNT                      ,
    CONS_RCV_HOLD_DURATION                  ,
    CONS_RCV_ACW_COUNT                      ,
    CONS_RCV_ACW_DURATION                  ,
    AGENT_TO_AGENT_CONS_COUNT              ,
    AGENT_TO_AGENT_CONS_DURATION          ,
    CREATE_AUDIT_KEY                     ,
    UPDATE_AUDIT_KEY                       ,
    START_DATE_TIME_KEY                     ,
    END_DATE_TIME_KEY                       ,
    START_TS                                ,
    END_TS                                  ,
    ACTIVE_FLAG                         ,
    PURGE_FLAG                       ,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_INTERACTION_RESOURCE_FACT C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.FT_INTERACTION_FACT)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;