CREATE TABLE UAT_DM.AR_rbt_with_subs AS
SELECT T2.subs_id, T1.* FROM UAT_DM.AR_rbt_init_dedup T1
JOIN prd2_dds_v.phone_number T2
ON T1.month_num = T2.report_date_month
AND T1.day_num = T2.report_date_day
AND T1.msisdn = T2.msisdn
AND T1.rn = 1
AND T2.report_date_year = 2018;

CREATE TABLE UAT_DM.ar_rbt_zeroes AS
SELECT DISTINCT SUBS_ID FROM UAT_DM.AR_rbt_with_subs
WHERE label = 0
AND month_num = 3;

INSERT INTO UAT_DM.AR_rbt_goodok_march_full_cl
SELECT * FROM new_cl_marts.new_cl_with_incr t1
where cl_year = 2018
and cl_month = 3
and cl_day = 1
and t1.subs_id IN (SELECT SUBS_ID FROM UAT_DM.ar_rbt_all_subs);

CREATE TABLE UAT_DM.ar_rbt_active_ones_zeroes AS
SELECT DISTINCT SUBS_ID FROM UAT_DM.AR_rbt_goodok_march_full_cl
WHERE last_voice_out < 30 OR last_msg_out < 30
;


CREATE TABLE UAT_DM.AR_rbt_goodok_march_full_cl_active_p LIKE UAT_DM.AR_rbt_goodok_march_full_cl_active STORED AS PARQUET

INSERT INTO UAT_DM.AR_rbt_goodok_march_full_cl_active_p
SELECT * FROM UAT_DM.AR_rbt_goodok_march_full_cl_active

CREATE TABLE UAT_DM.AR_rbt_for_chisq AS
SELECT t1.dest_number, COUNT(t2.label) cnt, AVG(t2.label) mean, t1.cat FROM 
(SELECT subs_id, dest_number, cat FROM UAT_DM.AR_rbt_goodok_march_full_cl_active_p) t1
JOIN
(
(SELECT subs_id, 1 label from UAT_DM.ar_rbt_ones)
UNION ALL
(SELECT subs_id, 0 label FROM UAT_DM.ar_rbt_zeroes)
) t2
on t1.subs_id = t2.subs_id
GROUP BY t1.dest_number, t1.cat