-- Create file format for extraction DSV
create or replace file format epam_lab.raw.lab_dsv_format
type = CSV
field_delimiter = '|'
skip_header = 1;

-- Create file format for extraction CSV
create or replace file format epam_lab.raw.lab_csv_format
type = CSV
field_delimiter = ','
skip_header = 1;
