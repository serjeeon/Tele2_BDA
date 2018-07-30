-- This splits your data into many comma separated files

insert overwrite directory '/tmp/out/ar_tinkoff_contacters_11_04_2018/' row format delimited fields terminated by ','
SELECT * FROM UAT_DM.tinkoff_combined_sample_1_contacters