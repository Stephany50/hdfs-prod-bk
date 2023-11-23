
---***********************************************************---
------------ CREATE IT Table- MAXIT_RAW_EVENT TT AND IT -------------------
---***********************************************************---

-- TT.MAXIT_RAW_EVENT
-- 2 colonnes en doublons venant du CDR brute. properties_pass_id properties_currency.
-- et les doublons ont été renommés en properties_pass_id_doublon properties_currency_doublon
drop table TT.MAXIT_RAW_EVENT;
CREATE EXTERNAL TABLE TT.MAXIT_RAW_EVENT (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
anonymousId VARCHAR(200),
context_app_build VARCHAR(200),
context_app_name VARCHAR(200),
context_app_namespace VARCHAR(200),
context_app_version VARCHAR(200),
context_device_adTrackingEnabled VARCHAR(200),
context_device_advertisingId VARCHAR(200),
context_device_id VARCHAR(200),
context_device_manufacturer VARCHAR(200),
context_device_model VARCHAR(200),
context_device_name VARCHAR(200),
context_device_type VARCHAR(200),
context_ip VARCHAR(200),
context_library_name VARCHAR(200),
context_library_version VARCHAR(200),
context_locale VARCHAR(200),
context_network_bluetooth VARCHAR(200),
context_network_carrier VARCHAR(200),
context_network_cellular VARCHAR(200),
context_network_wifi VARCHAR(200),
context_os_name VARCHAR(200),
context_os_version VARCHAR(200),
context_protocols_sourceId VARCHAR(200),
context_screen_density VARCHAR(200),
context_screen_height VARCHAR(200),
context_screen_width VARCHAR(200),
context_timezone VARCHAR(200),
context_traits_anonymousId VARCHAR(200),
context_traits_tenantId VARCHAR(200),
context_traits_userId VARCHAR(200),
context_userAgent VARCHAR(200),
properties_banner_category VARCHAR(200),
properties_banner_id VARCHAR(200),
properties_banner_name VARCHAR(200),
properties_build VARCHAR(200),
properties_bundle_category VARCHAR(200),
properties_bundle_id VARCHAR(200),
properties_bundle_name VARCHAR(200),
properties_category_name VARCHAR(200),
properties_context VARCHAR(200),
properties_currency VARCHAR(200),
properties_favorite_event VARCHAR(200),
properties_favorite_partner VARCHAR(200),
properties_favorite_services VARCHAR(200),
properties_filter_type VARCHAR(200),
properties_from_background VARCHAR(200),
properties_helper VARCHAR(200),
properties_item_category VARCHAR(200),
properties_item_category_id VARCHAR(200),
properties_item_duration VARCHAR(200),
properties_item_end_date VARCHAR(200),
properties_item_format VARCHAR(200),
properties_item_id VARCHAR(200),
properties_item_name VARCHAR(200),
properties_item_price VARCHAR(200),
properties_item_provider VARCHAR(200),
properties_item_provider_id VARCHAR(200),
properties_item_start_date VARCHAR(200),
properties_item_stock VARCHAR(200),
properties_item_subcategory VARCHAR(200),
properties_item_subtype VARCHAR(200),
properties_item_type VARCHAR(200),
properties_line_commitment VARCHAR(200),
properties_merchant_id VARCHAR(200),
properties_new_line VARCHAR(200),
properties_organism_title VARCHAR(200),
properties_organism_type VARCHAR(200),
properties_partner_id VARCHAR(200),
properties_pass_id VARCHAR(200),
properties_pass_name VARCHAR(200),
properties_pass_price VARCHAR(200),
properties_pass_type_id VARCHAR(200),
properties_pass_type_name VARCHAR(200),
properties_payment_method VARCHAR(200),
properties_payment_status VARCHAR(200),
properties_receiver_id VARCHAR(200),
properties_referring_application VARCHAR(200),
properties_request_status VARCHAR(200),
properties_request_type VARCHAR(200),
properties_sender_id VARCHAR(200),
properties_service_category VARCHAR(200),
properties_service_id VARCHAR(200),
properties_service_name VARCHAR(200),
properties_sos_activate_date VARCHAR(200),
properties_sos_activate_time VARCHAR(200),
properties_sos_id VARCHAR(200),
properties_sos_name VARCHAR(200),
properties_sos_remaining_balance VARCHAR(200),
properties_sos_transfer_amount VARCHAR(200),
properties_subscription_end_date VARCHAR(200),
properties_subscription_start_date VARCHAR(200),
properties_subscription_status VARCHAR(200),
properties_tenant_id VARCHAR(200),
properties_ticket_name0 VARCHAR(200),
properties_ticket_name1 VARCHAR(200),
properties_ticket_name2 VARCHAR(200),
properties_ticket_name3 VARCHAR(200),
properties_ticket_name4 VARCHAR(200),
properties_ticket_price0 VARCHAR(200),
properties_ticket_price1 VARCHAR(200),
properties_ticket_price2 VARCHAR(200),
properties_ticket_price3 VARCHAR(200),
properties_ticket_price4 VARCHAR(200),
properties_ticket_quantity0 VARCHAR(200),
properties_ticket_quantity1 VARCHAR(200),
properties_ticket_quantity2 VARCHAR(200),
properties_ticket_quantity3 VARCHAR(200),
properties_ticket_quantity4 VARCHAR(200),
properties_total_amount VARCHAR(200),
properties_total_price VARCHAR(200),
properties_transaction_amount VARCHAR(200),
properties_transaction_date VARCHAR(200),
properties_transaction_type VARCHAR(200),
properties_url VARCHAR(200),
properties_username VARCHAR(200),
properties_version VARCHAR(200),
properties_vote_price VARCHAR(200),
properties_voting_category VARCHAR(200),
properties_voting_content VARCHAR(200),
properties_bill_amount VARCHAR(200),
properties_bill_expiry_date VARCHAR(200),
properties_bill_id VARCHAR(200),
properties_bill_issue_date VARCHAR(200),
properties_bill_type VARCHAR(200),
properties_credit_remaining_balance VARCHAR(200),
properties_credit_transfer_amount VARCHAR(200),
properties_data_transfer_amount VARCHAR(200),
properties_data_unit VARCHAR(200),
properties_filter_name VARCHAR(200),
properties_option_id VARCHAR(200),
properties_option_name VARCHAR(200),
properties_option_remaining_balance VARCHAR(200),
properties_search_method VARCHAR(200),
properties_sharing_channel VARCHAR(200),
properties_social_network VARCHAR(200),
properties_permission_type VARCHAR(200),
properties_reason VARCHAR(200),
properties_pass_type VARCHAR(200),
properties_top_up_method VARCHAR(200),
properties_Partner_id_doublon VARCHAR(200),
properties_Currency_doublon VARCHAR(200),
properties_rate_confirmation VARCHAR(200),
properties_logout_confirmation VARCHAR(200),
properties_favorite_events_tickets VARCHAR(200),
properties_favorite_partners VARCHAR(200),
properties_purchased_services VARCHAR(200),
properties_avatar_id VARCHAR(200),
properties_rate_score VARCHAR(200),
properties_feedback_msg VARCHAR(200),
properties_app_list VARCHAR(200),
properties_app_name VARCHAR(200),
properties_app_desc VARCHAR(200),
properties_redir_link VARCHAR(200),
properties_websites_list VARCHAR(200),
properties_website_name VARCHAR(200),
properties_claim_id VARCHAR(200),
properties_claim_status VARCHAR(200),
properties_claim_text VARCHAR(200),
properties_claim_date VARCHAR(200),
properties_claim_time VARCHAR(200),
properties_remaining_balance VARCHAR(200),
properties_review_text VARCHAR(200),
properties_rating_stars VARCHAR(200),
properties_available_balance VARCHAR(200),
properties_transfer_amount VARCHAR(200),
properties_reason_selected VARCHAR(200),
properties_ticket_status VARCHAR(200),
properties_receipt_number VARCHAR(200),
properties_amount_received VARCHAR(200),
properties_from_account VARCHAR(200),
properties_receiver_account VARCHAR(200),
properties_active_filters VARCHAR(200),
properties_shop_name VARCHAR(200),
properties_shop_adress VARCHAR(200),
properties_open_closed VARCHAR(200),
properties_gender VARCHAR(200),
properties_first_name VARCHAR(200),
properties_last_name VARCHAR(200),
properties_birth_date VARCHAR(200),
properties_current_profession VARCHAR(200),
properties_city_residence VARCHAR(200),
properties_id_number VARCHAR(200),
properties_issuance_date VARCHAR(200),
properties_expiration_date VARCHAR(200),
properties_place_issue VARCHAR(200),
properties_field_activity VARCHAR(200),
properties_income_level VARCHAR(200),
properties_rightsholder VARCHAR(200),
event_type VARCHAR(200),
integrations_Firebase VARCHAR(200),
messageId VARCHAR(200),
name VARCHAR(200),
`originalTimestamp` VARCHAR(200),
receivedAt VARCHAR(200),
sentAt VARCHAR(200),
`timestamp` VARCHAR(200),
type VARCHAR(200),
userId VARCHAR(200),
writeKey VARCHAR(200),
version VARCHAR(200)
)
COMMENT 'external tables-TT for SPARK_IT_MAXIT_RAW_EVENT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/MAXIT_RAW_EVENT'
TBLPROPERTIES ('serialization.null.format'='');


-- CDR.SPARK_IT_MAXIT_RAW_EVENT
-- 2 colonnes en doublons venant du CDR brute. properties_pass_id properties_currency.
-- et les doublons ont été renommés en properties_pass_id_doublon properties_currency_doublon
CREATE TABLE CDR.SPARK_IT_MAXIT_RAW_EVENT (
anonymousId VARCHAR(200),
context_app_build VARCHAR(200),
context_app_name VARCHAR(200),
context_app_namespace VARCHAR(200),
context_app_version VARCHAR(200),
context_device_adTrackingEnabled VARCHAR(200),
context_device_advertisingId VARCHAR(200),
context_device_id VARCHAR(200),
context_device_manufacturer VARCHAR(200),
context_device_model VARCHAR(200),
context_device_name VARCHAR(200),
context_device_type VARCHAR(200),
context_ip VARCHAR(200),
context_library_name VARCHAR(200),
context_library_version VARCHAR(200),
context_locale VARCHAR(200),
context_network_bluetooth VARCHAR(200),
context_network_carrier VARCHAR(200),
context_network_cellular VARCHAR(200),
context_network_wifi VARCHAR(200),
context_os_name VARCHAR(200),
context_os_version VARCHAR(200),
context_protocols_sourceId VARCHAR(200),
context_screen_density VARCHAR(200),
context_screen_height VARCHAR(200),
context_screen_width VARCHAR(200),
context_timezone VARCHAR(200),
context_traits_anonymousId VARCHAR(200),
context_traits_tenantId VARCHAR(200),
context_traits_userId VARCHAR(200),
context_userAgent VARCHAR(200),
properties_banner_category VARCHAR(200),
properties_banner_id VARCHAR(200),
properties_banner_name VARCHAR(200),
properties_build VARCHAR(200),
properties_bundle_category VARCHAR(200),
properties_bundle_id VARCHAR(200),
properties_bundle_name VARCHAR(200),
properties_category_name VARCHAR(200),
properties_context VARCHAR(200),
properties_currency VARCHAR(200),
properties_favorite_event VARCHAR(200),
properties_favorite_partner VARCHAR(200),
properties_favorite_services VARCHAR(200),
properties_filter_type VARCHAR(200),
properties_from_background VARCHAR(200),
properties_helper VARCHAR(200),
properties_item_category VARCHAR(200),
properties_item_category_id VARCHAR(200),
properties_item_duration VARCHAR(200),
properties_item_end_date VARCHAR(200),
properties_item_format VARCHAR(200),
properties_item_id VARCHAR(200),
properties_item_name VARCHAR(200),
properties_item_price VARCHAR(200),
properties_item_provider VARCHAR(200),
properties_item_provider_id VARCHAR(200),
properties_item_start_date VARCHAR(200),
properties_item_stock VARCHAR(200),
properties_item_subcategory VARCHAR(200),
properties_item_subtype VARCHAR(200),
properties_item_type VARCHAR(200),
properties_line_commitment VARCHAR(200),
properties_merchant_id VARCHAR(200),
properties_new_line VARCHAR(200),
properties_organism_title VARCHAR(200),
properties_organism_type VARCHAR(200),
properties_partner_id VARCHAR(200),
properties_pass_id VARCHAR(200),
properties_pass_name VARCHAR(200),
properties_pass_price VARCHAR(200),
properties_pass_type_id VARCHAR(200),
properties_pass_type_name VARCHAR(200),
properties_payment_method VARCHAR(200),
properties_payment_status VARCHAR(200),
properties_receiver_id VARCHAR(200),
properties_referring_application VARCHAR(200),
properties_request_status VARCHAR(200),
properties_request_type VARCHAR(200),
properties_sender_id VARCHAR(200),
properties_service_category VARCHAR(200),
properties_service_id VARCHAR(200),
properties_service_name VARCHAR(200),
properties_sos_activate_date VARCHAR(200),
properties_sos_activate_time VARCHAR(200),
properties_sos_id VARCHAR(200),
properties_sos_name VARCHAR(200),
properties_sos_remaining_balance VARCHAR(200),
properties_sos_transfer_amount VARCHAR(200),
properties_subscription_end_date VARCHAR(200),
properties_subscription_start_date VARCHAR(200),
properties_subscription_status VARCHAR(200),
properties_tenant_id VARCHAR(200),
properties_ticket_name0 VARCHAR(200),
properties_ticket_name1 VARCHAR(200),
properties_ticket_name2 VARCHAR(200),
properties_ticket_name3 VARCHAR(200),
properties_ticket_name4 VARCHAR(200),
properties_ticket_price0 VARCHAR(200),
properties_ticket_price1 VARCHAR(200),
properties_ticket_price2 VARCHAR(200),
properties_ticket_price3 VARCHAR(200),
properties_ticket_price4 VARCHAR(200),
properties_ticket_quantity0 VARCHAR(200),
properties_ticket_quantity1 VARCHAR(200),
properties_ticket_quantity2 VARCHAR(200),
properties_ticket_quantity3 VARCHAR(200),
properties_ticket_quantity4 VARCHAR(200),
properties_total_amount VARCHAR(200),
properties_total_price VARCHAR(200),
properties_transaction_amount VARCHAR(200),
properties_transaction_date VARCHAR(200),
properties_transaction_type VARCHAR(200),
properties_url VARCHAR(200),
properties_username VARCHAR(200),
properties_version VARCHAR(200),
properties_vote_price VARCHAR(200),
properties_voting_category VARCHAR(200),
properties_voting_content VARCHAR(200),
properties_bill_amount VARCHAR(200),
properties_bill_expiry_date VARCHAR(200),
properties_bill_id VARCHAR(200),
properties_bill_issue_date VARCHAR(200),
properties_bill_type VARCHAR(200),
properties_credit_remaining_balance VARCHAR(200),
properties_credit_transfer_amount VARCHAR(200),
properties_data_transfer_amount VARCHAR(200),
properties_data_unit VARCHAR(200),
properties_filter_name VARCHAR(200),
properties_option_id VARCHAR(200),
properties_option_name VARCHAR(200),
properties_option_remaining_balance VARCHAR(200),
properties_search_method VARCHAR(200),
properties_sharing_channel VARCHAR(200),
properties_social_network VARCHAR(200),
properties_permission_type VARCHAR(200),
properties_reason VARCHAR(200),
properties_pass_type VARCHAR(200),
properties_top_up_method VARCHAR(200),
properties_Partner_id_doublon VARCHAR(200),
properties_Currency_doublon VARCHAR(200),
properties_rate_confirmation VARCHAR(200),
properties_logout_confirmation VARCHAR(200),
properties_favorite_events_tickets VARCHAR(200),
properties_favorite_partners VARCHAR(200),
properties_purchased_services VARCHAR(200),
properties_avatar_id VARCHAR(200),
properties_rate_score VARCHAR(200),
properties_feedback_msg VARCHAR(200),
properties_app_list VARCHAR(200),
properties_app_name VARCHAR(200),
properties_app_desc VARCHAR(200),
properties_redir_link VARCHAR(200),
properties_websites_list VARCHAR(200),
properties_website_name VARCHAR(200),
properties_claim_id VARCHAR(200),
properties_claim_status VARCHAR(200),
properties_claim_text VARCHAR(200),
properties_claim_date VARCHAR(200),
properties_claim_time VARCHAR(200),
properties_remaining_balance VARCHAR(200),
properties_review_text VARCHAR(200),
properties_rating_stars VARCHAR(200),
properties_available_balance VARCHAR(200),
properties_transfer_amount VARCHAR(200),
properties_reason_selected VARCHAR(200),
properties_ticket_status VARCHAR(200),
properties_receipt_number VARCHAR(200),
properties_amount_received VARCHAR(200),
properties_from_account VARCHAR(200),
properties_receiver_account VARCHAR(200),
properties_active_filters VARCHAR(200),
properties_shop_name VARCHAR(200),
properties_shop_adress VARCHAR(200),
properties_open_closed VARCHAR(200),
properties_gender VARCHAR(200),
properties_first_name VARCHAR(200),
properties_last_name VARCHAR(200),
properties_birth_date VARCHAR(200),
properties_current_profession VARCHAR(200),
properties_city_residence VARCHAR(200),
properties_id_number VARCHAR(200),
properties_issuance_date VARCHAR(200),
properties_expiration_date VARCHAR(200),
properties_place_issue VARCHAR(200),
properties_field_activity VARCHAR(200),
properties_income_level VARCHAR(200),
properties_rightsholder VARCHAR(200),
event_type VARCHAR(200),
integrations_Firebase VARCHAR(200),
messageId VARCHAR(200),
name VARCHAR(200),
`originalTimestamp` VARCHAR(200),
receivedAt TIMESTAMP,
sentAt TIMESTAMP,
`timestamp` TIMESTAMP,
type VARCHAR(200),
userId VARCHAR(200),
writeKey VARCHAR(200),
version TIMESTAMP,
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERTED_DATE TIMESTAMP,
ORIGINAL_FILE_DATE DATE
)COMMENT 'CDR_SPARK_IT_MAXIT_RAW_EVENT'
PARTITIONED BY (CDR_DATE_VERSION DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

truncate table CDR.SPARK_IT_MAXIT_RAW_EVENT ;

refresh table CDR.SPARK_IT_MAXIT_RAW_EVENT ;
show partitions CDR.SPARK_IT_MAXIT_RAW_EVENT ;
alter table CDR.SPARK_IT_MAXIT_RAW_EVENT drop partition (CDR_DATE_VERSION="2023-11-09");
alter table CDR.SPARK_IT_MAXIT_RAW_EVENT drop partition (CDR_DATE_VERSION="2023-11-10");
alter table CDR.SPARK_IT_MAXIT_RAW_EVENT drop partition (CDR_DATE_VERSION="2023-11-11");
alter table CDR.SPARK_IT_MAXIT_RAW_EVENT drop partition (CDR_DATE_VERSION="__HIVE_DEFAULT_PARTITION__");

select cdr_date_version, count(*) from CDR.SPARK_IT_MAXIT_RAW_EVENT  where cdr_date_version > "2023-10-08" group by 1 order by 1;

select * from CDR.SPARK_IT_MAXIT_RAW_EVENT  where cdr_date_version = "2023-11-09" limit 4;
select * from CDR.SPARK_IT_MAXIT_RAW_EVENT  where cdr_date_version is null limit 4;

 select cdr_date_version, count(*) from  CDR.SPARK_IT_MAXIT_RAW_EVENT  where original_file_name = "MAXIT_RAW_EVENT_20231110000000.csv" group by 1;
