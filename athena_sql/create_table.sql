CREATE EXTERNAL TABLE `deribi`(
  `time` string,
  `open` float,
  `close` float,
  `high` float,
  `low` float)
PARTITIONED BY (
  `kind` string,
  `instrument` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://deribi-instruments/ingestion'
TBLPROPERTIES (
  'classification'='csv',
  'transient_lastDdlTime'='1667497592')