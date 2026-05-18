import os
from datetime import datetime

import pandas as pd
import requests


API_URL = os.getenv("ORDERS_API_URL", "http://96.9.212.102:8000/orders")
OUTPUT_DIR = os.getenv(
    "ORDERS_RAW_PATH",
    "/opt/airflow/data_lake/orders/raw",
)
OUTPUT_FORMAT = os.getenv("ORDERS_OUTPUT_FORMAT", "parquet").lower()

ORDER_COLUMNS = [
    "order_id",
    "user_id",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "eval_set",
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "add_to_cart_order",
    "reordered",
]


def flatten_orders(payload):
    rows = []

    for order in payload.get("orders", []):
        base_order = {
            "order_id": order.get("order_id"),
            "user_id": order.get("user_id"),
            "order_number": order.get("order_number"),
            "order_dow": order.get("order_dow"),
            "order_hour_of_day": order.get("order_hour_of_day"),
            "days_since_prior_order": order.get("days_since_prior_order"),
            "eval_set": order.get("eval_set"),
        }

        for product in order.get("products") or []:
            rows.append(
                {
                    **base_order,
                    "product_id": product.get("product_id"),
                    "product_name": product.get("product_name"),
                    "aisle_id": product.get("aisle_id"),
                    "aisle": product.get("aisle"),
                    "department_id": product.get("department_id"),
                    "department": product.get("department"),
                    "add_to_cart_order": product.get("add_to_cart_order"),
                    "reordered": product.get("reordered"),
                }
            )

    return rows


def write_staged_file(df, current_time):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if OUTPUT_FORMAT == "csv":
        output_path = os.path.join(OUTPUT_DIR, f"orders_{current_time}.csv")
        df.to_csv(output_path, index=False)
        return output_path

    output_path = os.path.join(OUTPUT_DIR, f"orders_{current_time}.parquet")
    try:
        df.to_parquet(output_path, index=False)
    except ImportError:
        output_path = os.path.join(OUTPUT_DIR, f"orders_{current_time}.csv")
        df.to_csv(output_path, index=False)

    return output_path


def fetch_orders_stream():
    print("Opening data stream: Orders API...")

    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        payload = response.json()

        rows = flatten_orders(payload)
        if not rows:
            raise ValueError("Orders API returned no product-level rows.")

        df = pd.DataFrame(rows, columns=ORDER_COLUMNS)

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = write_staged_file(df, current_time)

        total_orders = payload.get("total_orders", "unknown")
        print(
            f"Saved {len(df)} rows from {total_orders} orders to {output_path}"
        )
    except Exception as exc:
        print(f"Failed to fetch orders data: {exc}")
        raise


if __name__ == "__main__":
    fetch_orders_stream()
