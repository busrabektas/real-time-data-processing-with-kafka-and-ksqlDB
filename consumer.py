import json
from kafka import KafkaConsumer
import smtplib
from email.mime.text import MIMEText
import os

KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'NUMBERS_GREATER_THAN_9'

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='email_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def send_email(number, timestamp):
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    sender_email = 'senderemail'
    receiver_email = 'receiveremail' 
    password = 'password'  

    subject = 'Number greater than 9 detected'
    body = f"Number: {number}, Timestamp: {timestamp}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)
        print(f"Email sent: Number {number} Timestamp {timestamp}")
    except Exception as e:
        print(f"Error while sending email: {e}")
    finally:
        server.quit()

try:
    for message in consumer:
        data = message.value
        number = data.get('NUMBER')  
        timestamp = data.get('TIMESTAMP')
        print(f"Alındı: {data}")
        send_email(number, timestamp)

except KeyboardInterrupt:
    print("\nMessage consume stopped.")
finally:
    consumer.close()
    print("Consumer closed.")
