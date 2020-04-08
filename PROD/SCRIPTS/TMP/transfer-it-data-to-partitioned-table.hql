

val subsSchemaTxt = Seq(
("original_file_name","string"),
("original_file_size","int"),
("original_file_line_count","int"),
("event_inst_id","bigint"),
("re_id","int"),
("billing_nbr","string"),
("billing_imsi","string"),
("calling_nbr","string"),
("called_nbr","string"),
("third_part_nbr","string"),
("start_time","timestamp"),
("duration","int"),
("lac_a","string"),
("cell_a","string"),
("lac_b","string"),
("cell_b","string"),
("calling_imei","string"),
("called_imei","string"),
("price_id1","int"),
("price_id2","int"),
("price_id3","int"),
("price_id4","int"),
("price_plan_id1","int"),
("price_plan_id2","int"),
("price_plan_id3","int"),
("price_plan_id4","int"),
("acct_res_id1","int"),
("acct_res_id2","int"),
("acct_res_id3","int"),
("acct_res_id4","int"),
("charge1","bigint"),
("charge2","bigint"),
("charge3","bigint"),
("charge4","bigint"),
("bal_id1","bigint"),
("bal_id2","bigint"),
("bal_id3","bigint"),
("bal_id4","bigint"),
("acct_item_type_id1","int"),
("acct_item_type_id2","int"),
("acct_item_type_id3","int"),
("acct_item_type_id4","int"),
("prepay_flag","tinyint"),
("pre_balance1","bigint"),
("balance1","bigint"),
("pre_balance2","bigint"),
("balance2","bigint"),
("pre_balance3","bigint"),
("balance3","bigint"),
("pre_balance4","bigint"),
("balance4","bigint"),
("international_roaming_flag","tinyint"),
("call_type","tinyint"),
("byte_up","bigint"),
("byte_down","bigint"),
("bytes","bigint"),
("price_plan_code","string"),
("session_id","string"),
("result_code","string"),
("prod_spec_std_code","string"),
("yzdiscount","int"),
("byzcharge1","bigint"),
("byzcharge2","bigint"),
("byzcharge3","bigint"),
("byzcharge4","bigint"),
("onnet_offnet","int"),
("provider_id","int"),
("prod_spec_id","int"),
("termination_cause","int"),
("b_prod_spec_id","string"),
("b_price_plan_code","string"),
("callspetype","int"),
("chargingratio","int"),
("dummy","string")
)
flux.input-schema += {"field": "original_file_name", "type": "string"}
flux.input-schema += {"field": "original_file_size", "type": "integer"}
flux.input-schema += {"field": "original_file_line_count", "type": "integer"}
flux.input-schema += {"field": "event_time", "type": "string"}
flux.input-schema += {"field": "ts", "type": "string"}
flux.input-schema += {"field": "connid", "type": "string"}
flux.input-schema += {"field": "ani", "type": "string"}
flux.input-schema += {"field": "dnis", "type": "string"}
flux.input-schema += {"field": "last_vq", "type": "string"}
flux.input-schema += {"field": "technical_result", "type": "string"}
flux.input-schema += {"field": "technical_result_code", "type": "string"}
flux.input-schema += {"field": "result_reason", "type": "string"}
flux.input-schema += {"field": "result_reason_code", "type": "string"}
flux.input-schema += {"field": "duree_conversation", "type": "integer"}
flux.input-schema += {"field": "place_key", "type": "integer"}
flux.input-schema += {"field": "ud_site_choisi", "type": "string"}
flux.input-schema += {"field": "integereraction_type", "type": "string"}
flux.input-schema += {"field": "integereraction_type_code", "type": "string"}
flux.input-schema += {"field": "place", "type": "string"}
flux.input-schema += {"field": "resource_type", "type": "string"}
flux.input-schema += {"field": "resource_name", "type": "string"}
flux.input-schema += {"field": "nom", "type": "string"}
flux.input-schema += {"field": "duree_file", "type": "integer"}


("original_file_name","string"),
("original_file_size","integer"),
("original_file_line_count","integer"),
("event_time","string"),
("ts","string"),
("connid","string"),
("ani","string"),
("dnis","string"),
("last_vq","string"),
("technical_result","string"),
("technical_result_code","string"),
("result_reason","string"),
("result_reason_code","string"),
("duree_conversation","integer"),
("place_key","integer"),
("ud_site_choisi","string"),
("integereraction_type","string"),
("integereraction_type_code","string"),
("place","string"),
("resource_type","string"),
("resource_name","string"),
("nom","string"),
("duree_file","integer")