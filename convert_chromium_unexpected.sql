
drop sequence if exists run_order_seq;
create sequence run_order_seq;

drop table if exists tests_unexpected;
create table tests_unexpected as 
select build_id as build, test_id as test_name, test_name as test_id, build_start_t as start_time, duration as execution_time,
 final_status, status, true as verdict, nextval('run_order_seq') as run_order from trybot_test_results 
where status in ('PASS', 'FAIL', 'CRASH', 'ABORT') and build_start_t > '2021-01-01' and build_start_t < '2021-02-01' and duration is not NULL
order by build_start_t asc, test_id;

update tests_unexpected set verdict = false where final_status = 'UNEXPECTED' and status in ('FAIL', 'CRASH');
alter table tests_unexpected drop column final_status;
alter table tests_unexpected drop column status;
alter table tests_unexpected add primary key (build, test_id);
