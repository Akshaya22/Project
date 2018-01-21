data1 = LOAD '/home/hduser/Downloads/project/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
data3= FILTER data1 BY case_status=='CERTIFIED' or case_status=='CERTIFIED-WITHDRAWN';
data4= FILTER data3 BY full_time_position=='Y';
data6= foreach data4 generate $1,$4,$5,$6,$7;
data7= group data6 by $4;
data8 = foreach data7 generate group,ROUND_TO(AVG(data6.prevailing_wage),2) as avg_wage;
dump data8;
