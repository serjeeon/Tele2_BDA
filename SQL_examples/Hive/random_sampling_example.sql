select * from <tablename>
where rand() <= 0.5 //approzimate sample size to full data ratio
distribute by rand()
sort by rand()
limit 5000000; //sample size
