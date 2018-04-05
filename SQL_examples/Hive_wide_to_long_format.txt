SELECT subs_id, var_name, var_value FROM
(SELECT subs_id, map(
 "lifetime", lifetime
, "firstcall_delay", firstcall_delay
, "age", age) as var_map FROM UAT_DM.AR_mini_dmsc) x
lateral view explode(var_map) exptbl1 as var_name, var_value