
drop sequence if exists run_order_seq;
create sequence run_order_seq;

drop table if exists tests;
create table tests as 
select build_id as build, test_id as test_name, test_name as test_id, build_start_t as start_time, duration as execution_time,
 final_status, status, true as verdict, nextval('run_order_seq') as run_order from trybot_test_results 
where status in ('PASS', 'FAIL', 'CRASH', 'ABORT') and build_start_t > '2021-01-01' and build_start_t < '2021-02-01' and duration is not NULL
order by build_start_t asc, test_id;

update tests set verdict = false where status in ('FAIL', 'CRASH');
alter table tests drop column final_status;
alter table tests drop column status;
alter table tests add primary key (build, test_id);
