h1bdata = load '/home/hduser/Downloads/project/h1b_final' using PigStorage('\t') as (s_no:int, case_status:chararray , employer_name:chararray , soc_name:chararray , job_title:chararray , full_time_position:chararray ,prevailing_wage:long ,year:chararray , worksite:chararray , longitute:double, latitute:double);
--*Calculate total applications for each year
year_grouped = group h1bdata BY $7;
year_total_count = FOREACH year_grouped GENERATE group,COUNT($1) as yearcount;
dump year_total_count;
--describe year_total_count;
--*Calculate total for each case status for each year
case_status_grouped_by_year = group h1bdata by ($7,$1);
dump case_status_grouped_by_year;
case_status_count = FOREACH case_status_grouped_by_year GENERATE group,group.$0,COUNT($1);
dump case_status_count;
--describe case_status_count;
--*Join the tables
joined = join case_status_count by $1,year_total_count by $0;
dump joined;
--describe joined;
--*Percent Calculation
percent = FOREACH joined GENERATE FLATTEN($0),$2 as casecount,(float) ($2*100)/($4) as casepercent;
dump percent
