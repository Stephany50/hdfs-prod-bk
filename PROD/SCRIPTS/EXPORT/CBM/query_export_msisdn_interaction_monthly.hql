SELECT
    msisdn,
    contact1, 
    contact2, 
    contact3,
    contact4,
    contact5, 
    contact6, 
    contact7, 
    contact8, 
    contact9,
    contact10, 
    contact11, 
    contact12, 
    contact13, 
    contact14, 
    contact15, 
    contact16, 
    contact17, 
    contact18, 
    contact19, 
    contact20,
    -- contact21,
    -- contact22,
    -- contact23,
    -- contact24,
    -- contact25,
    -- contact26,
    -- contact27,
    -- contact28,
    -- contact29,
    -- contact30,
    -- contact31,
    -- contact32,
    -- contact33,
    -- contact34,
    -- contact35,
    -- contact36,
    -- contact37,
    -- contact38,
    -- contact39,
    -- contact40,
    -- contact41,
    -- contact42,
    -- contact43,
    -- contact44,
    -- contact45,
    -- contact46,
    -- contact47,
    -- contact48,
    -- contact49,
    -- contact50,
    event_month
FROM MON.SPARK_FT_MSISDNS_INTERACTION_MONTH
WHERE event_month = '###SLICE_VALUE###'
AND FN_GET_OPERATOR_CODE(msisdn) = 'OCM'
