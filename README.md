# AWS Glue/Athena Demo
> This demo setup AWS Step Functions to orchestrate a sample ETL workflow using AWS Glue and Athena.

## The ETL workflow (from Step Functions)

![](https://raw.githubusercontent.com/peteragility/athena-etl-demo/master/diagram/athena-step-functions.png)

## To deploy this demo Cloudformation stack into your AWS account
> Just click next for every step in Cloudformation stack deployment:
- To deploy the Cloudformation stack to **AWS Singapore region**, click [here](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-1#/stacks/new?stackName=athena-etl-demo-stack&templateURL=https://hkt-aws-quick-start.s3.ap-east-1.amazonaws.com/athena-etl-demo/athena-ctas-flow.yaml).
- To deploy the Cloudformation stack to **AWS Hong Kong region**, click [here](https://console.aws.amazon.com/cloudformation/home?region=ap-east-1#/stacks/new?stackName=athena-etl-demo-stack&templateURL=https://hkt-aws-quick-start.s3.ap-east-1.amazonaws.com/athena-etl-demo/athena-ctas-flow.yaml). (**For new AWS account you need to enable Hong Kong region before it can be used, for detail please refer to this [doc](https://docs.aws.amazon.com/general/latest/gr/rande-manage.html#rande-manage-enable).**)

## What AWS resources are deployed in this stack?
After the Cloudformation stack run successfully, you will the below resources created in your AWS account:
- An **Amazon S3** bucket prefixed "athena-demo-" is created, raw data and curated data are stored in this bucket.
- Two **AWS Glue** crawlers are created, one for csv data and one for json data. Crawler will detect the schema/metadata of the data files stored in Glue metadata store (compatible to Hive metadata). Athena, Redshift and EMR can then leverage this metadata for query processing on Amazon S3.
- Two **AWS Lambda** functions, they are created to trigger the AWS Glue crawlers.
- An state machine in **AWS Step Functions** is created for Athena ETL workflow. This state machine will call the Glue crawlers and then execute SQL queries in Athena.

## How to run the Demo?
1. Git clone this repo to your local workstation's directory, for example: "/my-athena-demo-dir".
2. Open AWS Console, goto Amazon S3 service (you can search any AWS service in the top left searching bar), find the S3 bucket named "athena-demo-*", click the bucket.
3. Click "Upload" to upload the data files, upload all the directories/files under /my-athena-demo-dir/data on your workstation (just drag and drop). Double check after upload there are two directories in the S3 bucket:
   - raw/
   - poc_ch_lookup/
4. Goto "Step Functions" in AWS Console, click the Athena state machine, and click "Start Execution", wait for all steps to complete (turn green). You can check each step and its related progress/logs in the Step Functions UI.
5. Now goto "Athena" in AWS Console, switch the workgroup to the workgroup named "athena-demo-workgroup-*", you will find all the raw data tables AND two curated tables in the table list on the left:
   - demo_order_table (partition by cretn_year and cretn_month)
   - demo_call_table (partition by start_year, start_month, start_day)
6. Curated table data are stored under the S3 bucket's curated/ folder, you can have a look and see how partitioned table data is stored in S3. The curated table's data is in columnar file format: Parquet, and is compressed by SNAPPY. The data partitioning, columnar file format and compression are critical for the best performance of SQL querying in Athena. For more tips in Athena performance tuning, can refer to this [AWS Blog](https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/).
7. You can now execute any SQL queries in the Athena console on the raw or curated tables, you can try any SQL queries Athena is a serverless Presto engine on top of Amazon S3, you can refer to the following docs for SQL and datetime conversion syntax:
   - [SQL Reference for Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html)
   - [Date and Time Functions and Operators on Presto](https://prestodb.io/docs/current/functions/datetime.html)
8. For data visualization/BI, you can use [Amazon Quicksight](https://aws.amazon.com/quicksight/), [Apache Superset on AWS](https://aws.amazon.com/quickstart/architecture/apache-superset/), Qilkview or Tableau, all of them have Athena connectors to get data from Athena.

## The Athena's SQL queries that actually run in the ETL flow
- For demo_order_table:
  ```sql
  CREATE table demo_order_table
  WITH (format='PARQUET',parquet_compression='SNAPPY', partitioned_by=array['cretn_year','cretn_month'],
  external_location = 's3://athena-demo-etl-xxx/curated/demo_order_table/')
  AS
  select a.order_id, a.cust_num, a.accnt_num, a.subr_num, a.mob_num, a.order_type_cd, a.order_stat, a.shop_num, 
  a.salman_num, a.cretn_date, a.completion_date, a.serv_req_date, a.cancellation_date, b.order_sub_id, b.order_action, 
  b.prod_id, p.prod_name, p.prod_desc, p.prod_type,p.onetime_fee,p.mthly_fee, c.doc_type_cd, c.acq_date, c.cust_type, 
  d.accnt_cretn_date, d.accnt_expiry_date, d.pay_meth_cd, s.salman_name, s.salman_team, k.shop_name, k.shop_addr, k.sal_chl, z.owner_cust_num, z.acq_date as subr_acq_date, 
  year(date_parse(cretn_date,'%m/%d/%Y')) as cretn_year, 
  month(date_parse(cretn_date,'%m/%d/%Y')) as cretn_month 
  from glue_hkt_order_header a, glue_hkt_order_detail b, glue_hkt_cust c, glue_hkt_prod p, 
  glue_hkt_accnt d, glue_hkt_salman s, glue_hkt_shop k, glue_hkt_subr z 
  where a.order_id = b.order_id 
  and a.cust_num = c.cust_num 
  and b.prod_id = p.prod_id 
  and a.accnt_num = d.accnt_num 
  and a.cust_num = d.cust_num 
  and a.salman_num = s.salman_num 
  and a.shop_num = k.shop_num 
  and a.subr_num = z.subr_num
  ```
- For demo_call_table:
  ```sql
  CREATE table demo_call_table 
  WITH (format='PARQUET', parquet_compression='SNAPPY', partitioned_by=array['start_year','start_month','start_day'] 
  external_location = 's3://athena-demo-etl-xxx/curated/demo_call_table/') 
  AS
  select a.user_id, a.channel_num, a.duration, b.channel_name, 
  date_parse(a.start_time,'%Y%m%d %H:%i:%s') as start_time, 
  date_parse(b.start_datetime,'%Y%m%d %H:%i:%s') as start_datetime, 
  date_parse(b.end_datetime,'%Y%m%d %H:%i:%s') as end_datetime, 
  year(date_parse(a.start_time,'%Y%m%d %H:%i:%s')) as start_year, 
  month(date_parse(a.start_time,'%Y%m%d %H:%i:%s')) as start_month, 
  day(date_parse(a.start_time,'%Y%m%d %H:%i:%s')) as start_day
  from glue_poc_tv_cdr_log a left join glue_poc_ch_lookup b 
  on a.CHANNEL_NUM = b.CHANNEL_NUM
  and date_parse(a.start_time,'%Y%m%d %H:%i:%s') >= date_parse(b.start_datetime,'%Y%m%d %H:%i:%s')
  and date_parse(a.start_time,'%Y%m%d %H:%i:%s') < date_parse(b.end_datetime,'%Y%m%d %H:%i:%s')
  ```

## Next steps?
- Run a self paced [Athena workshop](https://athena-in-action.workshop.aws/20-howtostart/201-self-paced.html).
- Athena is one of the tools to handle SQL query/ETL on S3's data lake, to learn more on other AWS services like Amazon Redshift, Amazon EMR, can follow [this workshop](https://intro-to-analytics-on-aws.workshop.aws/en/lab-guide/ingest.html).
