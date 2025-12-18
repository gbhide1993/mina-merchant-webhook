import os
import redis
from flask import Flask, request
from twilio.rest import Client
from rq import Queue
from twilio.twiml.messaging_response import MessagingResponse

app = Flask(__name__)

# --- CONFIGURATION ---
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# --- REDIS CONNECTION ---
# This connects to the Redis instance shared with the Worker
conn = redis.from_url(REDIS_URL)
q = Queue(connection=conn)

def send_ack_message(to_number, body_text):
    """
    Sends an immediate acknowledgment via Twilio API
    so the user knows we got the message.
    """
    try:
        client = Client(TWILIO_SID, TWILIO_AUTH)
        client.messages.create(
            from_=TWILIO_NUMBER,
            to=to_number,
            body=body_text
        )
    except Exception as e:
        print(f"❌ Ack Send Error: {e}")

@app.route("/whatsapp", methods=['POST'])
def whatsapp_webhook():
    """
    The Listener.
    Receives the message, sends a quick reply, and pushes the job to the Worker.
    """
    # 1. Extract Data (Exactly as per original app logic)
    raw_data = {
        'body': request.values.get('Body', '').strip(),
        'from': request.values.get('From', ''),
        'num_media': int(request.values.get('NumMedia', 0)),
        'media_url': request.values.get('MediaUrl0'),
        'media_type': request.values.get('MediaContentType0'),
        'message_sid': request.values.get('MessageSid')
    }

    sender = raw_data['from']
    print(f"📩 Webhook received msg from {sender}")

    # 2. Immediate User Feedback (Prevent Timeout & Improve UX)
    # We send this NOW so Twilio doesn't wait for the Worker
    if raw_data['num_media'] > 0 and 'audio' in raw_data.get('media_type', ''):
        send_ack_message(sender, "👂 Sun raha hoon... (Listening)")
        
    elif raw_data['num_media'] > 0 and 'image' in raw_data.get('media_type', ''):
        send_ack_message(sender, "👀 Image mil gayi... (Processing)")

    # 3. Enqueue the Job
    # The 'tasks_merchant.process_message' string tells the Worker 
    # exactly which function to run in the other repo.
    q.enqueue('tasks_merchant.process_message', raw_data)

    # 4. Return TwiML (Standard Empty Response)
    return str(MessagingResponse())

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)