CREATE OR REPLACE TABLE
 dataset-381617.mekari_sandbox.cost_per_hour_data AS
WITH
 timesheet AS (
 SELECT
   timesheet_id,
   employee_id,
   date,
   checkin,
   checkout,
   ROW_NUMBER() OVER(PARTITION BY employee_id, date ORDER BY timesheet_id) AS rn ---Deleted Duplicate Data (one employee_id has two checkin checkout data)
 FROM
   dataset-381617.mekari_sandbox.timesheets QUALIFY rn=1),
 employee AS (
 SELECT
   employe_id,
   branch_id,
   join_date,
   resign_date,
   MAX(salary) salary ---Deleted Duplicate Data (one employee_id has two salary, only use maximum salary)
 FROM
   dataset-381617.mekari_sandbox.employees
 GROUP BY
   1,
   2,
   3,
   4),
 ---Join timesheet and employee data
 core_data AS (
 SELECT
   a.timesheet_id,
   a.employee_id,
   b.branch_id,
   b.salary,
   b.join_date,
   b.resign_date,
   a.date,
   a.checkin,
   a.checkout,
   CASE
     WHEN a.checkin<=a.checkout THEN TIME_DIFF(a.checkout,a.checkin, second)
     WHEN a.checkin>a.checkout THEN DATETIME_DIFF(DATETIME(DATE_ADD(a.date, INTERVAL 1 day), a.checkout), DATETIME(a.date,a.checkin), second)
 END
   actual_working_hour_in_second,
   28800 AS expected_working_hour_in_second  ---expected working hour in a day is 8 hour
 FROM
   timesheet a
 LEFT JOIN
   employee b
 ON
   a.employee_id=b.employe_id
 WHERE
   date<=IFNULL(resign_date, CURRENT_DATE()) ),
 ---Get median working hour per employee_id to impute missing and irrelevant data
 median_working_hour_data AS (
 SELECT
   DISTINCT employee_id,
   branch_id,
   PERCENTILE_DISC(actual_working_hour_in_second, 0.5) OVER (PARTITION BY employee_id, branch_id) median_working_hour
 FROM
   core_data ),
 ---Get salary data per branch_id and monthly
 salary_data AS (
 SELECT
   month_key,
   branch_id,
   SUM(salary) salary
 FROM (
   SELECT
     DATE_TRUNC(date,month) AS month_key,
     branch_id,
     employee_id,
     salary
   FROM
     core_data
   GROUP BY
     1,
     2,
     3,
     4)
 GROUP BY
   1,
   2),
 ---Get working hour data per branch_id and monthly
 working_hour_data AS (
 SELECT
   DATE_TRUNC(date,month) AS month_key,
   branch_id,
   SUM(actual_working_hour_in_second)/3600 actual_working_hour,
   ---convert second to hour
   SUM(expected_working_hour_in_second)/3600 expected_working_hour  ---convert second to hour
 FROM (
   SELECT
     timesheet_id,
     employee_id,
     branch_id,
     salary,
     join_date,
     resign_date,
     date,
     checkin,
     checkout,
     CASE
       WHEN actual_working_hour_in_second IS NULL THEN median_working_hour
       WHEN actual_working_hour_in_second>39600 THEN 39600  ---If working hour>11 hour then 11 (based on goverment regulation)
       WHEN actual_working_hour_in_second<14400 THEN 14400  ---If working hour<4 hour then 4 (half day leave)
       ELSE actual_working_hour_in_second
   END
     actual_working_hour_in_second,
     expected_working_hour_in_second
   FROM
     core_data
   LEFT JOIN
     median_working_hour_data
   USING
     (employee_id,
       branch_id))
 GROUP BY
   1,
   2)
 ---Get cost per hour data per branch_id and monthly
SELECT
 branch_id,
 month_key,
 salary,
 actual_working_hour,
 expected_working_hour,
 CAST(salary/actual_working_hour AS INT64) actual_cost_per_hour,
 CAST(salary/expected_working_hour AS INT64) expected_cost_per_hour
FROM
 working_hour_data
LEFT JOIN
 salary_data
USING
 (month_key,
   branch_id)
ORDER BY
 1,
 2
