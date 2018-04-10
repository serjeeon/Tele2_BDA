REPLACE PROCEDURE UAT_DM.AR_get_CL_tinkoff_credit(IN batchname VARCHAR(30), IN offset INTEGER, IN depth INTEGER) 
BEGIN DECLARE rd DATE; L1: FOR cc AS c_cust CURSOR FOR SELECT report_date FROM UAT_DM.AR_CL_load 
GROUP BY 1 DO SET rd = cc.report_date; INSERT INTO UAT_DM.AR_CL_lite SELECT subs_id, CASE WHEN REGEXP_INSTR(contact,'(^\d{11}$|^\d{14}$)')=1 THEN '7'||SUBSTR(contact, LENGTH(contact)-9,10) ELSE contact END c2 
 , batchname FROM (SELECT subs_id, usage_type_id, rd report_date, CASE WHEN USAGE_TYPE_ID = 2 THEN A_NUMBER ELSE B_NUMBER END contact , DURATION,traffic_type_id 
 FROM PRD2_DDS_V.USAGE_CDR b WHERE b.start_date <=rd-offset AND b.start_date>=(rd-depth-offset) AND SUBS_ID IN (SEL SUBS_ID FROM UAT_DM.AR_CL_load WHERE report_date=rd) AND traffic_type_id IN (1,2,3) AND contact IN (SEL MSISDN FROM UAT_DM.AR_tinkoff_credit_numbers)
 UNION ALL SELECT subs_id, usage_type_id, rd report_date , CASE WHEN USAGE_TYPE_ID = 2 THEN A_NUMBER ELSE B_NUMBER END contact, DURATION, traffic_type_id 
 FROM PRD2_DDS_V.USAGE_BILLING b WHERE b.start_date <=rd-offset AND b.start_date>=(rd-depth-offset) AND SUBS_ID IN ( SEL SUBS_ID FROM UAT_DM.AR_CL_load WHERE report_date=rd) AND traffic_type_id IN (1,2,3) AND contact IN (SEL MSISDN FROM UAT_DM.AR_tinkoff_credit_numbers)
 ) t
 GROUP BY SUBS_ID, c2, report_date
 ; END FOR L1; END;
 
REPLACE PROCEDURE UAT_DM.AR_get_CL_tinkoff_cards_lite(IN batchname VARCHAR(30), IN offset INTEGER, IN depth INTEGER) 
BEGIN DECLARE rd DATE; L1: FOR cc AS c_cust CURSOR FOR SELECT report_date FROM UAT_DM.AR_CL_Load_tinkoff_cards
GROUP BY 1 DO SET rd = cc.report_date; INSERT INTO UAT_DM.AR_CL_lite SELECT subs_id, CASE WHEN REGEXP_INSTR(contact,'(^\d{11}$|^\d{14}$)')=1 THEN '7'||SUBSTR(contact, LENGTH(contact)-9,10) ELSE contact END c2 
 , batchname FROM (SELECT subs_id, usage_type_id, rd report_date, CASE WHEN USAGE_TYPE_ID = 2 THEN A_NUMBER ELSE B_NUMBER END contact , DURATION,traffic_type_id 
 FROM PRD2_DDS_V.USAGE_CDR b WHERE b.start_date <=rd-offset AND b.start_date>=(rd-depth-offset) AND SUBS_ID IN (SEL SUBS_ID FROM UAT_DM.AR_CL_Load_tinkoff_cards WHERE report_date=rd) AND traffic_type_id IN (1,2,3) AND contact IN (SEL MSISDN FROM UAT_DM.AR_tinkoff_cards_msisdns)
 UNION ALL SELECT subs_id, usage_type_id, rd report_date , CASE WHEN USAGE_TYPE_ID = 2 THEN A_NUMBER ELSE B_NUMBER END contact, DURATION, traffic_type_id 
 FROM PRD2_DDS_V.USAGE_BILLING b WHERE b.start_date <=rd-offset AND b.start_date>=(rd-depth-offset) AND SUBS_ID IN ( SEL SUBS_ID FROM UAT_DM.AR_CL_Load_tinkoff_cards WHERE report_date=rd) AND traffic_type_id IN (1,2,3) AND contact IN (SEL MSISDN FROM UAT_DM.AR_tinkoff_cards_msisdns)
 ) t
 GROUP BY SUBS_ID, c2, report_date
 ; END FOR L1; END;
 
									