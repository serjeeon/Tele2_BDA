/* creating table with partitions.

This is necessary to spread the table across the amps.
It is reasonable to do it only if you have big tables, where there multiple records per user.

There are two possibilities: when you have a range of values (like dates) or when you have categories (like branches).
In case of dates you need to define a period so that all dates are inside this period.
For categories you need to define all unique categories.

!Important. Partitioning only makes sense when users (or you) use them. If no one will select values using partitions, it won't help.

You can define 1 or more partitions.
*/

CREATE MULTISET TABLE UAT_DM.al_prediction_vendor_bucket
,NO FALLBACK
,NO BEFORE JOURNAL
,NO AFTER JOURNAL
as (
SELECT report_date, subs_id, model_id, model_version, probability,
       QUANTILE(99, rnk) + 1 AS BUCKET_VALUE, segment_id, load_id
  FROM (
        SELECT CURRENT_DATE - 3 AS report_date,
               subs_id,
               194 as model_id,
               1 as model_version,
               cal_p1_class_1 AS probability,
               1 as segment_id,
               0 as load_id, 
               ROW_NUMBER() OVER (ORDER BY cal_p1_class_1 DESC) AS rnk 
          FROM UAT_DM.al_prediction_vendor
         WHERE cal_p1_class_1 > 0 ) t1
) WITH NO DATA
PRIMARY INDEX (subs_id)
PARTITION BY ( RANGE_N(report_date  BETWEEN DATE '2018-09-10' AND DATE '2018-09-20' EACH INTERVAL '1' DAY ),
CASE_N(
segment_id =  1 ,
segment_id =  2 ,
segment_id =  3 ,
segment_id =  4 ,
segment_id =  5 ,
 NO CASE OR UNKNOWN) );