
	  
SEL distinct key_name, NAME_TAG, reg_name, S1.OSM_ID, S1.MULTIPOLYGON, S2.MULTIPOLYGON, S1.cellid FROM 
	(SEL * FROM 
		(SEL OSM_ID, reg_name, key_name,
 			MULTIPOLYGON, MULTIPOLYGON.ST_MBR_Xmin() xmin, MULTIPOLYGON.ST_MBR_Xmax() xmax, MULTIPOLYGON.ST_MBR_Ymin() ymin, MULTIPOLYGON.ST_MBR_Ymax() ymax
    		FROM UAT_DM.VM_20180322_null_region_place_centroids_2) 
		A
  		, TABLE(tessellate(A.OSM_ID, A.xmin, A.ymin, A.xmax, A.ymax,
    	-180,  40, 180, 80, 100, 100 )) 
	T1
    	WHERE A.OSM_ID = T1.out_key ) 
S1,
    (SEL * FROM 
		(SEL OSM_ID, NAME_TAG, MULTIPOLYGON, MULTIPOLYGON.ST_MBR_Xmin() xmin, MULTIPOLYGON.ST_MBR_Xmax() xmax, MULTIPOLYGON.ST_MBR_Ymin() ymin, MULTIPOLYGON.ST_MBR_Ymax() ymax
      		FROM UAT_DM.VM_20180323_MISSING_DISTR_MULTIPOLYGONS) 
		B
  		, TABLE(tessellate(B.OSM_ID, B.xmin, B.ymin, B.xmax, B.ymax,
    	-180,  40, 180, 80, 100, 100 ))
		
	T1
    WHERE B.OSM_ID = T1.out_key) 
S2
    WHERE S1.cellid = S2.cellid
      AND S1.xmax >= S2.xmin and S1.xmin <= S2.xmax
      AND S1.ymax >= S2.ymin and S1.ymin <= S2.ymax
	  AND S2.MULTIPOLYGON.ST_Intersects(S1.MULTIPOLYGON) = 1
	  

