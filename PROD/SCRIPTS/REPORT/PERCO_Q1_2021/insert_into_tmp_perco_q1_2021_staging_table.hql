insert into tmp.perco_q1_2021_staging_table
select
    site_name,
    avg(takers_best_deal_offer_daily) avg_takers_best_deal_offer_daily,
    avg(takers_best_deal_voice_offer_daily) avg_takers_best_deal_voice_offer_daily,
    avg(takers_best_deal_data_offer_daily) avg_takers_best_deal_data_offer_daily,
    avg(takers_best_deal_combo_offer_daily) avg_takers_best_deal_combo_offer_daily,

    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_best_deal_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_voice_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_best_deal_voice_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_data_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_best_deal_data_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_combo_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_best_deal_combo_offer_mtd,

    avg(subscriptions_best_deal_offer_daily) avg_subscriptions_best_deal_offer_daily,
    avg(subscriptions_best_deal_voice_offer_daily) avg_subscriptions_best_deal_voice_offer_daily,
    avg(subscriptions_best_deal_data_offer_daily) avg_subscriptions_best_deal_data_offer_daily,
    avg(subscriptions_best_deal_combo_offer_daily) avg_subscriptions_best_deal_combo_offer_daily,

    avg(takers_myway_plus_daily) avg_takers_myway_plus_daily,
    avg(takers_myway_plus_voice_offer_daily) avg_takers_myway_plus_voice_offer_daily,
    avg(takers_myway_plus_data_offer_daily) avg_takers_myway_plus_data_offer_daily,
    avg(takers_myway_plus_combo_offer_daily) avg_takers_myway_plus_combo_offer_daily,
    avg(takers_myway_plus_via_om_daily) avg_takers_myway_plus_via_om_daily,

    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_myway_plus_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_voice_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_myway_plus_voice_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_data_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_myway_plus_data_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_combo_offer_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_myway_plus_combo_offer_mtd,
    sum(
        case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_via_om_mtd end
    ) / count(
        distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
    ) avg_takers_myway_plus_via_om_mtd,

    avg(subscriptions_myway_plus_daily) avg_subscriptions_myway_plus_daily,
    avg(subscriptions_myway_plus_voice_offer_daily) avg_subscriptions_myway_plus_voice_offer_daily,
    avg(subscriptions_myway_plus_data_offer_daily) avg_subscriptions_myway_plus_data_offer_daily,
    avg(subscriptions_myway_plus_combo_offer_daily) avg_subscriptions_myway_plus_combo_offer_daily,
    avg(subscriptions_myway_plus_via_om_daily) avg_subscriptions_myway_plus_via_om_daily
from tmp.perco_q1_2021_incrementals
group by site_name