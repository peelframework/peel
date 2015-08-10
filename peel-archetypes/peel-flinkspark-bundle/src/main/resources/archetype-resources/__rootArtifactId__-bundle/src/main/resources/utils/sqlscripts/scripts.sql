#Runtime average, median, min, max and number of runs for all experiments
select e.name, e.suite, CAST(ROUND(AVG(r.time)/1000.0,2) as NUMERIC(18,2)) as average, MEDIAN(r.time)/1000.0 as median, MAX(r.time)/1000.0 as max, MIN(r.time)/1000.0 as min, COUNT(*) as numberOfRuns from experiment e, experiment_run r where e.id = r.experiment_id group by e.suite, e.name order by e.name, e.suite;

select e.name, e.suite, r.time as runtime, run from experiment e, experiment_run r where e.id = r.experiment_id;