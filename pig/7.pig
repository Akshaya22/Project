data = LOAD '/home/hduser/Downloads/project/h1b_final' USING PigStorage('\t') AS (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);
a = group data by year;
b = foreach a generate FLATTEN(group), COUNT(data.year) as number;
STORE b INTO 'home/hduser/Downloads/project7' USING PigStorage('\t');
