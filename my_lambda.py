import os
import json
import boto3
from newsapi import NewsApiClient
from datetime import datetime
from kafka import KafkaProducer

def lambda_handler(event, context):
    # News API Key
    API_KEY = os.environ.get("NEWS_API_KEY",)
    if not API_KEY:
        return {"statusCode": 500, "body": "Missing NEWS_API_KEY"}

    # Init clients
    newsapi = NewsApiClient(api_key=API_KEY)
    headlines = newsapi.get_top_headlines(country="us")

    # Save to S3
    s3 = boto3.client('s3')
    filename = "news_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"
    s3.put_object(
        Bucket="bucketname",
        Key=f"foldername/{filename}",
        Body=json.dumps(headlines)
    )

    # Send to Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BROKER", "ec2 ip address:9092"),  # Use EC2 IP
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Kafka connection failed: {str(e)}"}

    topic = os.environ.get("KAFKA_TOPIC", "topic name")
    for article in headlines.get("articles", []):
        producer.send(topic, value=article)
    producer.flush()

    return {
        "statusCode": 200,
        "body": f"✅ Saved to S3 and sent {len(headlines.get('articles', []))} articles to Kafka topic '{topic}'"
    }
