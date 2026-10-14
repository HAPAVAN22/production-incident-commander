
import concurrent.futures
import random
import time

import requests

BASE_URL = "http://localhost:8000"
PAYMENT_URL = f"{BASE_URL}/api/v1/payments"

TOTAL_REQUESTS = 100
WORKERS = 10
FAILURE_PERCENTAGE = 60

CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "SGD", "AUD", "CAD"]
METHOD = ["CARD", "UPI", "WALLET"]

# Replace with a valid payload for your payment API.
def build_payload():
    return {
        "customer_id": "550e8400-e29b-41d4-a716-446655440000",
        "amount": random.randint(100, 5000),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(METHOD),
    }


def send_payment(_):
    should_fail = random.random() < FAILURE_PERCENTAGE / 100

    headers = {}
    if should_fail:
        headers["X-Simulate-Failure"] = "true"

    try:
        response = requests.post(
            PAYMENT_URL,
            json=build_payload(),
            headers=headers,
            timeout=10,
        )

        return response.status_code

    except requests.RequestException:
        return "CONNECTION_ERROR"


start = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor(
    max_workers=WORKERS
) as executor:
    results = list(executor.map(send_payment, range(TOTAL_REQUESTS)))

elapsed = time.perf_counter() - start

print(f"Requests sent: {len(results)}")
print(f"Duration: {elapsed:.2f}s")
print(f"HTTP 2xx: {sum(isinstance(x, int) and 200 <= x < 300 for x in results)}")
print(f"HTTP 5xx: {sum(isinstance(x, int) and 500 <= x < 600 for x in results)}")
print(f"HTTP 4xx: {sum(isinstance(x, int) and 400 <= x < 500 for x in results)}")
print(f"Connection errors: {results.count('CONNECTION_ERROR')}")
