INSERT INTO agg.spark_ft_credit_transfer_b2b

SELECT 
TRANSFER_ID
,REFILL_TIME                 
,SENDER_MSISDN                  
,COMMERCIAL_OFFER               
,RECEIVER_IMSI                
,RECEIVER_MSISDN                 
,TRANSFER_VOLUME                 
,TRANSFER_VOLUME_UNIT                 
,SENDER_DEBIT_AMT                   
,TRANSFER_AMT                   
,TRANSFER_FEES                  
,TRANSFER_MEAN                  
,TERMINATION_IND                               
,SENDER_OPERATOR_CODE                   
,RECEIVER_OPERATOR_CODE
,ORIGINAL_FILE_NAME            
,INSERT_DATE
,REFILL_DATE

FROM
(SELECT * FROM  MON.SPARK_FT_CREDIT_TRANSFER 
where refill_date ='###SLICE_VALUE###' and  trim(upper(COMMERCIAL_OFFER)) in 
(trim('ACCESS PRO+'),
trim('ARAMIS'),
trim('ATHOS'),
trim('CLOUD BEW20K'),
trim('CLOUD GOLD'),
trim('CLOUD INTENSE'),
trim('CLOUD INTENSE PLUS'),
trim('CLOUD MAX'),
trim('CLOUD MOIS'),
trim('CLOUD MOIS +'),
trim('CLOUD MOIS ++'),
upper(trim('Data Live')),
trim('DATA LIVE 10MO'),
trim('DATA LIVE 13MO'),
trim('DATA LIVE 30MO'),
trim('DATA LIVE 3MO'),
trim('DATA LIVE 5MO'),
trim('DATA LIVE 7MO'),
trim('DATA LIVE ILLIMITÉ WITHOUT SMS'),
trim('DATA LIVE MIX 10MO IHS'),
trim('DATA LIVE MIX BUNDLE L1'),
trim('DATA LIVE MIX BUNDLE L2'),
trim('DATA LIVE MIX BUNDLE L3'),
trim('DATA LIVE MIX BUNDLE L4'),
trim('DATA LIVE MIX BUNDLE L5'),
trim('DATA LIVE MIX BUNDLE L6'),
trim('DATA LIVE MIX SMARTRACK'),
upper(trim('DataLive')),
trim('ENTREPRENEUR MEDIUM'),
trim('FLEX 100K'),
trim('FLEX 10K'),
trim('FLEX 125K'),
trim('FLEX 12K'),
trim('FLEX 150K'),
trim('FLEX 15K'),
trim('FLEX 16.5K'),
trim('FLEX 20K'),
trim('FLEX 22K'),
trim('FLEX 250K'),
trim('FLEX 25K'),
trim('FLEX 27.5K'),
trim('FLEX 30K'),
trim('FLEX 35K'),
trim('FLEX 38.5K'),
trim('FLEX 3K'),
trim('FLEX 40K'),
trim('FLEX 45K'),
trim('FLEX 50K'),
trim('FLEX 55K'),
trim('FLEX 5K'),
trim('FLEX 60K'),
trim('FLEX 65K'),
trim('FLEX 70K'),
trim('FLEX 75K'),
trim('FLEX 80K'),
trim('FLEX 90K'),
trim('FLEX PLUS 100K'),
trim('FLEX PLUS 10K'),
trim('FLEX PLUS 10K UN'),
trim('FLEX PLUS 115K'),
trim('FLEX PLUS 120K'),
trim('FLEX PLUS 130K'),
trim('FLEX PLUS 150K'),
trim('FLEX PLUS 15K'),
trim('FLEX PLUS 175K'),
trim('FLEX PLUS 190K'),
trim('FLEX PLUS 200K'),
trim('FLEX PLUS 20K'),
trim('FLEX PLUS 20K UN'),
trim('FLEX PLUS 250K'),
trim('FLEX PLUS 25K'),
trim('FLEX PLUS 30K'),
trim('FLEX PLUS 30K UN'),
trim('FLEX PLUS 35K'),
trim('FLEX PLUS 3K'),
trim('FLEX PLUS 40K'),
trim('FLEX PLUS 4K'),
trim('FLEX PLUS 50K'),
trim('FLEX PLUS 55K'),
trim('FLEX PLUS 5K'),
trim('FLEX PLUS 5K UN'),
trim('FLEX PLUS 60K'),
trim('FLEX PLUS 65K'),
trim('FLEX PLUS 70K'),
trim('FLEX PLUS 75K'),
trim('FLEX PLUS 80K'),
trim('FLEX PLUS 85K'),
trim('FLEX PLUS 90K'),
trim('FLEX PREMIUM 100K'),
trim('FLEX PREMIUM 25K'),
trim('FLEX PREMIUM 2K'),
trim('FLEX PREMIUM 30K'),
trim('FLEX PREMIUM 40K'),
trim('FLEX PREMIUM 50K'),
trim('FLEX PREMIUM 75K'),
trim('FLYBOX SIM'),
upper(trim('Forfait internet mobile 20k prepaid')),
upper(trim('Forfait mix 4k')),
trim('FORFAIT MIX PLATINUM'),
trim('GEOPRO'),
trim('GEOPRO PREMIUM'),
trim('GPRS TRACKING'),
upper(trim('Grands Comptes')),
trim('HOTLINE ENTREPRISE VERT'),
trim('HYBRID ORANGE PRO BUNDLE PLUS'),
trim('INTENSE SCHLUMBERGER'),
trim('INTENSE128'),
trim('INTENSE16'),
trim('INTENSE32'),
trim('INTENSE64'),
trim('INTENSE8'),
trim('INTENSECORPRATE'),
trim('INTENSETOTAL'),
upper(trim('Mix Bronze')),
upper(trim('Mix Diamond')),
upper(trim('Mix Gold')),
upper(trim('Mix Silver')),
trim('OPEN 100K'),
trim('OPEN 10K'),
trim('OPEN 125K'),
trim('OPEN 150K'),
trim('OPEN 20K'),
trim('OPEN 25K'),
trim('OPEN 30K'),
trim('OPEN 35K'),
trim('OPEN 40K'),
trim('OPEN 45K'),
trim('OPEN 50K'),
trim('OPEN 5K'),
trim('OPEN 60K'),
trim('OPEN 80K'),
trim('OPEN 85K'),
trim('OPEN PLUS 100K'),
trim('OPEN PLUS 120K'),
trim('OPEN PLUS 130K'),
trim('OPEN PLUS 150K'),
trim('OPEN PLUS 20K'),
trim('OPEN PLUS 250K'),
trim('OPEN PLUS 40K'),
trim('OPEN PLUS 55K'),
trim('OPEN PLUS 60K'),
trim('OPEN PLUS 70K'),
trim('OPEN PLUS 80K'),
trim('OPEN PLUS 85K'),
trim('ORANGE ACCESS COMMUNITY'),
trim('ORANGE BUSINESS FLEX MODULO'),
upper(trim('Orange corporate Classique')),
trim('ORANGE FLYBOX PRO'),
upper(trim('Orange Grand Compte Move')),
trim('ORANGE GRAND COMPTE MOVE MIX'),
trim('ORANGE INTENSE HONORABLE'),
trim('ORANGE MIX HONORABLE'),
trim('ORANGE MIX SCHLUMBERGER'),
trim('ORANGE PREMIUM GC MIXTE'),
trim('ORANGE PRO L'),
trim('ORANGE PRO M'),
trim('ORANGE PRO S'),
trim('ORANGE PRO XL'),
trim('PERSONNEL TOTAL'),
trim('PORTHOS'),
upper(trim('PostPaid Open Plus')),
upper(trim('PostPaid Pro Open')),
upper(trim('Prepaid Access Plus')),
upper(trim('PrePaid Cloud Pro')),
trim('PREPAID COMMUNAUTE MAGIC'),
upper(trim('Prepaid Flex Plus')),
upper(trim('Prepaid Mobility')),
trim('PREPAID ORANGE PRO BUNDLE'),
trim('PREPAID PRO /ACCESS'),
upper(trim('PrePaid Pro Flex')),
trim('RICHELIEU'),
trim('ROUTAGE APPELS INTER'),
trim('SIM OPEL MAIN PRODUCT') ) ) RF