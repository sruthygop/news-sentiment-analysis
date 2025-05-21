CREATE OR REPLACE DATABASE news_sentiment_analysis;
USE DATABASE news_sentiment_analysis;


CREATE OR REPLACE SCHEMA sentiment_schema;
USE SCHEMA sentiment_schema;



 CREATE OR REPLACE TABLE news_sentiment_articles (
    source_name STRING,
    author STRING,
    title STRING,
    description STRING,
    url STRING,
    urlToImage STRING,
    publishedAt TIMESTAMP_NTZ,
    content STRING,
    sentiment FLOAT
);


CREATE OR REPLACE FILE FORMAT news_sentiment_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('NULL', 'null')
  TRIM_SPACE = TRUE;

---------------------------------------------------

 CREATE OR REPLACE STAGE news_sentiment_stage
  URL = 's3://bucke tname/foldername/'
  CREDENTIALS = (
    AWS_KEY_ID = 'aws_key_id'
    AWS_SECRET_KEY = 'aws_secret_id'
  )
  FILE_FORMAT = news_sentiment_format;

--------------------------------------
  
CREATE OR REPLACE TASK load_s3_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON */5 * * * * UTC'  -- every 5 minutes
AS
COPY INTO news_sentiment_articles
FROM @news_sentiment_stage
FILE_FORMAT = (FORMAT_NAME = 'news_sentiment_format')
ON_ERROR = 'CONTINUE';


ALTER TASK load_s3_task RESUME;
LIST @news_sentiment_stage;
SELECT * FROM news_sentiment_articles
