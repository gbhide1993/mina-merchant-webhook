#app_merchant.py (FINAL – Merchant Webhook)

from flask import Flask, request
import traceback
import uuid

from utils import send_whatsapp, upload_twilio_media_to_gcs
from db_merchant import (
    get_or_create_merchant_by_phone,
    create_transcription_job,
)


app = Flask(__name__)

# -------------------------
# TWILIO WHATSAPP WEBHOOK
# -------------------------

@app.route("/twilio/webhook", methods=["POST"])
def twilio_merchant_webhook():
    form = request.values

    sender = form.get("From")                  # whatsapp:+91XXXXXXXXXX
    media_url = form.get("MediaUrl0")
    media_type = form.get("MediaContentType0") or ""

    # Safety: ignore invalid requests
    if not sender or not sender.startswith("whatsapp:"):
        return ("", 204)

    phone = sender.replace("whatsapp:", "")

    # Ensure merchant exists
    merchant = get_or_create_merchant_by_phone(phone)

    # -------------------------
    # HANDLE VOICE NOTES ONLY
    # -------------------------
    if media_url and media_type.startswith("audio/"):
        try:
            gcs_path = upload_twilio_media_to_gcs(
                media_url=media_url,
                content_type=media_type,
                phone=phone
            )

            job_id = str(uuid.uuid4())

            create_transcription_job(
                job_id=job_id,
                merchant_id=merchant["id"],
                phone=phone,
                gcs_path=gcs_path,
                status="pending"
            )

            send_whatsapp(
                phone,
                "🎤 Audio mil gaya.\nYaad rakh raha hoon…"
            )

        except Exception as e:
            print("Merchant webhook error:", traceback.format_exc())
            send_whatsapp(
                phone,
                "⚠️ Audio receive karne mein problem aayi.\nEk baar phir bhejiye."
            )

        return ("", 204)

    # -------------------------
    # EVERYTHING ELSE: IGNORE
    # -------------------------
    return ("", 204)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
