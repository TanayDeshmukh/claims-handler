import os

import requests
from dotenv import load_dotenv

from common.utils import get_logger
from generate_dummy_invoice import generate_bicycle_insurance_invoice

load_dotenv()

def send_mock_email():
    logger = get_logger()

    url = os.getenv("EMAIL_INGEST_URL")

    payload = {
        "sender": "mock@bicycle_dealer.com",
        "subject": "Request to process the insurance claim",
        "body": "Dummy text"
    }
    dummy_claim = generate_bicycle_insurance_invoice()


    files = {
        "attachment": ("dummy_claim.pdf", dummy_claim, "application/pdf")
    }

    response = requests.post(url=url, data=payload, files=files)
    logger.info(f"{response.status_code}, {response.json()}")


if __name__ == '__main__':
    send_mock_email()