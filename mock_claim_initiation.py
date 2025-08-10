import os
import argparse
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
        "body": "Dummy text",
    }
    dummy_claim = generate_bicycle_insurance_invoice()

    files = {"attachment": ("dummy_claim.pdf", dummy_claim, "application/pdf")}

    response = requests.post(url=url, data=payload, files=files)
    logger.info(f"{response.status_code}, {response.json()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run with a specified number of samples."
    )

    parser.add_argument(
        "--num_samples",
        type=int,
        default=10,
        help="Number of dummy claim samples to be generated.",
    )

    args = parser.parse_args()

    num_samples = args.num_samples

    assert num_samples < 100

    for i in range(num_samples):
        print(f"Mocking email {i+1}/{num_samples}...")
        send_mock_email()
