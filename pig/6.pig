data = LOAD '/home/hduser/Downloads/project/h1b_final' USING PigStorage(',') AS (s_no:int,case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int,year:chararray, worksite:chararray, longitute:double, latitute:double);
a = group data by $7;
total = foreach a generate group,group.$0,COUNT($1);
b = group data by ($7,$1);
countyear = foreach b generate group,group.$0,COUNT($1);
joined = join countyear by $1, total by $0;
ans = foreach joined generate FLATTEN($0),(float)($2*100)/$4,$2;
dump ans;
