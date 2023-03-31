CREATE SCHEMA
  mekari_sandbox;
CREATE TABLE
  employees ( employee_id INT PRIMARY KEY,
    branch_id INT NOT NULL,
    salary DECIMAL NOT NULL,
    join_date DATE NOT NULL,
    resign_date DATE);
CREATE TABLE
  timesheets ( timesheet_id BIGINT NOT NULL PRIMARY KEY,
    employee_id INT NOT NULL,
    date DATE,
    checkin TIME,
    checkout TIME);
