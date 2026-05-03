from __future__ import annotations

import logging
import json
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator
from supabase import Client, create_client


DUFFEL_BASE_URL = "https://api.duffel.com"
DUFFEL_VERSION = "v2"
SIMULATION_MIN_DELTA = Decimal("-50.00")
SIMULATION_MAX_DELTA = Decimal("50.00")
STABLE_THRESHOLD_PCT = Decimal("0.50")
ALERT_DROP_THRESHOLD_PCT = Decimal("3.00")
DEFAULT_TIMEOUT_SECONDS = 30

ROUTES: list[tuple[str, str]] = [
    ("WAW", "LHR"),
    ("WAW", "JFK"),
    ("WAW", "DXB"),
]


@dataclass(frozen=True)
class RawOffer:
    route: str
    origin: str
    destination: str
    departure_date: str
    airline: str
    currency: str
    original_price: Decimal
    booking_link: str


class FlightPriceRecord(BaseModel):
    route: str = Field(min_length=7)
    origin: str = Field(min_length=3, max_length=3)
    destination: str = Field(min_length=3, max_length=3)
    departure_date: date
    airline: str = Field(min_length=1)
    currency: str = Field(min_length=3, max_length=3)
    original_price: Decimal = Field(gt=Decimal("0"))
    simulated_price: Decimal = Field(gt=Decimal("0"))
    price_change_pct: Decimal
    trend: str
    booking_link: str = Field(min_length=1)

    @field_validator("trend")
    @classmethod
    def validate_trend(cls, value: str) -> str:
        allowed = {"UP", "DOWN", "STABLE"}
        if value not in allowed:
            raise ValueError(f"trend must be one of {allowed}")
        return value


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def decimal_round(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def get_target_departure_date() -> str:
    return (date.today() + timedelta(days=90)).isoformat()


def create_supabase_client() -> Client:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_key = os.getenv("SUPABASE_KEY", "").strip()
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set")
    return create_client(supabase_url, supabase_key)


def duffel_headers(duffel_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {duffel_token}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def create_offer_request(
    duffel_token: str,
    origin: str,
    destination: str,
    departure_date: str,
) -> str | None:
    payload = {
        "data": {
            "slices": [
                {
                    "origin": origin,
                    "destination": destination,
                    "departure_date": departure_date,
                }
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }

    try:
        response = requests.post(
            f"{DUFFEL_BASE_URL}/air/offer_requests",
            headers=duffel_headers(duffel_token),
            json=payload,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get("id")
    except requests.RequestException as exc:
        logging.error(
            "Duffel offer request failed for %s-%s: %s",
            origin,
            destination,
            exc,
        )
        return None


def fetch_offer_for_request(duffel_token: str, offer_request_id: str) -> dict[str, Any] | None:
    params = {"offer_request_id": offer_request_id, "limit": 1}
    try:
        response = requests.get(
            f"{DUFFEL_BASE_URL}/air/offers",
            headers=duffel_headers(duffel_token),
            params=params,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        offers = response.json().get("data", [])
        if not offers:
            return None
        return offers[0]
    except requests.RequestException as exc:
        logging.error("Duffel offers fetch failed for request %s: %s", offer_request_id, exc)
        return None


def parse_raw_offer(offer: dict[str, Any], origin: str, destination: str, departure_date: str) -> RawOffer | None:
    try:
        owner_name = offer.get("owner", {}).get("name", "Unknown Airline")
        total_amount = Decimal(str(offer.get("total_amount")))
        total_currency = str(offer.get("total_currency", "USD")).upper()
        booking_link = str(offer.get("booking_requirements", {}).get("conditions", "https://www.google.com/travel/flights"))

        return RawOffer(
            route=f"{origin}-{destination}",
            origin=origin,
            destination=destination,
            departure_date=departure_date,
            airline=owner_name,
            currency=total_currency,
            original_price=decimal_round(total_amount),
            booking_link=booking_link,
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        logging.error("Failed to parse Duffel offer payload: %s", exc)
        return None


def extract_offers(duffel_token: str) -> list[RawOffer]:
    departure_date = get_target_departure_date()
    extracted: list[RawOffer] = []

    for origin, destination in ROUTES:
        request_id = create_offer_request(
            duffel_token=duffel_token,
            origin=origin,
            destination=destination,
            departure_date=departure_date,
        )
        if not request_id:
            continue

        offer_payload = fetch_offer_for_request(duffel_token, request_id)
        if not offer_payload:
            logging.warning("No offers returned for route %s-%s", origin, destination)
            continue

        raw_offer = parse_raw_offer(offer_payload, origin, destination, departure_date)
        if raw_offer:
            extracted.append(raw_offer)

    return extracted


def simulate_market_price(original_price: Decimal) -> Decimal:
    random_delta = Decimal(str(random.uniform(float(SIMULATION_MIN_DELTA), float(SIMULATION_MAX_DELTA))))
    simulated = original_price + random_delta
    if simulated <= Decimal("0"):
        simulated = Decimal("0.01")
    return decimal_round(simulated)


def fetch_last_route_record(supabase: Client, route: str) -> dict[str, Any] | None:
    try:
        response = (
            supabase.table("flight_prices")
            .select("*")
            .eq("route", route)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        records = response.data or []
        return records[0] if records else None
    except Exception as exc:  # noqa: BLE001 - preserving DB errors in logs
        logging.error("Supabase query failed for route %s: %s", route, exc)
        return None


def compute_price_change_and_trend(previous_price: Decimal | None, current_price: Decimal) -> tuple[Decimal, str]:
    if previous_price is None or previous_price <= Decimal("0"):
        return Decimal("0.00"), "STABLE"

    change_pct = decimal_round(((current_price - previous_price) / previous_price) * Decimal("100"))
    if change_pct > STABLE_THRESHOLD_PCT:
        trend = "UP"
    elif change_pct < -STABLE_THRESHOLD_PCT:
        trend = "DOWN"
    else:
        trend = "STABLE"
    return change_pct, trend


def transform_offer(raw_offer: RawOffer, previous_record: dict[str, Any] | None) -> FlightPriceRecord | None:
    simulated_price = simulate_market_price(raw_offer.original_price)
    previous_price_raw = previous_record.get("simulated_price") if previous_record else None
    previous_price = Decimal(str(previous_price_raw)) if previous_price_raw is not None else None

    price_change_pct, trend = compute_price_change_and_trend(previous_price, simulated_price)

    try:
        record = FlightPriceRecord(
            route=raw_offer.route,
            origin=raw_offer.origin,
            destination=raw_offer.destination,
            departure_date=raw_offer.departure_date,
            airline=raw_offer.airline,
            currency=raw_offer.currency,
            original_price=raw_offer.original_price,
            simulated_price=simulated_price,
            price_change_pct=price_change_pct,
            trend=trend,
            booking_link=raw_offer.booking_link,
        )
    except ValidationError as exc:
        logging.error("Pydantic validation failed for route %s: %s", raw_offer.route, exc)
        return None

    logging.info(
        "Fetched original price %s %s, simulated market price %s %s, Trend: %s",
        record.original_price,
        record.currency,
        record.simulated_price,
        record.currency,
        record.trend,
    )
    return record


def load_record(supabase: Client, record: FlightPriceRecord) -> bool:
    payload = record.model_dump(mode="json")
    try:
        supabase.table("flight_prices").insert(payload).execute()
        return True
    except Exception as exc:  # noqa: BLE001 - preserving DB errors in logs
        logging.error("Supabase insert failed for route %s: %s", record.route, exc)
        return False


def build_quickchart_url(old_price: Decimal, new_price: Decimal, currency: str) -> str:
    chart_config = {
        "type": "bar",
        "data": {
            "labels": ["Previous", "Current"],
            "datasets": [
                {
                    "label": f"Price ({currency})",
                    "data": [float(old_price), float(new_price)],
                    "backgroundColor": ["#9ca3af", "#ef4444"],
                }
            ],
        },
        "options": {
            "plugins": {"legend": {"display": False}},
            "scales": {"y": {"beginAtZero": False}},
        },
    }
    encoded_config = quote_plus(json.dumps(chart_config, separators=(",", ":")))
    return f"https://quickchart.io/chart?c={encoded_config}"


def render_drop_alert_html(
    route_from: str,
    route_to: str,
    old_price: Decimal,
    new_price: Decimal,
    currency: str,
    drop_pct: Decimal,
    chart_url: str,
    booking_link: str,
) -> str:
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #1f2937; line-height: 1.5;">
        <p>Hello,</p>
        <p>
          We detected a significant price drop for route <strong>{route_from} → {route_to}</strong>.
        </p>
        <ul>
          <li><strong>Previous price:</strong> {old_price} {currency}</li>
          <li><strong>New price:</strong> {new_price} {currency}</li>
          <li><strong>Drop:</strong> {drop_pct}%</li>
        </ul>
        <p>Price trend chart:</p>
        <p>
          <img src="{chart_url}" alt="Price change chart for {route_from}-{route_to}" style="max-width: 520px; width: 100%;" />
        </p>
        <p>
          Review details and continue booking:
          <a href="{booking_link}">Open airline offer</a>
        </p>
        <p>Best regards,<br/>Flight Intelligence Pipeline</p>
      </body>
    </html>
    """.strip()


def send_email_alert(
    route_from: str,
    route_to: str,
    old_price: Decimal,
    new_price: Decimal,
    currency: str,
    drop_pct: Decimal,
    booking_link: str,
) -> None:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    sender_email = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev").strip()
    recipient_email = os.getenv("ALERT_EMAIL_TO", "").strip()

    if not api_key:
        logging.warning("RESEND_API_KEY is missing. Skipping alert email.")
        return
    if not recipient_email:
        logging.warning("ALERT_EMAIL_TO is missing. Skipping alert email.")
        return

    chart_url = build_quickchart_url(old_price=old_price, new_price=new_price, currency=currency)
    html_body = render_drop_alert_html(
        route_from=route_from,
        route_to=route_to,
        old_price=old_price,
        new_price=new_price,
        currency=currency,
        drop_pct=drop_pct,
        chart_url=chart_url,
        booking_link=booking_link,
    )
    subject = f"📉 Price Drop Alert: {route_from} to {route_to}!"

    try:
        import resend

        resend.api_key = api_key
        resend.Emails.send(
            {
                "from": sender_email,
                "to": [recipient_email],
                "subject": subject,
                "html": html_body,
            }
        )
        logging.info("Email alert sent for route %s-%s", route_from, route_to)
    except Exception as exc:  # noqa: BLE001 - keep pipeline running on alert failures
        logging.error("Failed to send email alert for route %s-%s: %s", route_from, route_to, exc)


def run_pipeline() -> None:
    load_dotenv()
    setup_logging()

    duffel_token = os.getenv("DUFFEL_TOKEN", "").strip()
    if not duffel_token:
        raise RuntimeError("DUFFEL_TOKEN must be set")

    supabase = create_supabase_client()
    offers = extract_offers(duffel_token)
    if not offers:
        logging.warning("No offers extracted. Pipeline finished without inserts.")
        return

    inserted = 0
    for raw_offer in offers:
        previous = fetch_last_route_record(supabase, raw_offer.route)
        record = transform_offer(raw_offer, previous)
        if not record:
            continue

        previous_price_raw = previous.get("simulated_price") if previous else None
        previous_price = Decimal(str(previous_price_raw)) if previous_price_raw is not None else None
        significant_drop = (
            previous_price is not None
            and record.trend == "DOWN"
            and abs(record.price_change_pct) > ALERT_DROP_THRESHOLD_PCT
        )
        if significant_drop:
            send_email_alert(
                route_from=record.origin,
                route_to=record.destination,
                old_price=previous_price,
                new_price=record.simulated_price,
                currency=record.currency,
                drop_pct=abs(record.price_change_pct),
                booking_link=record.booking_link,
            )

        if load_record(supabase, record):
            inserted += 1

    logging.info("Pipeline completed. Inserted rows: %s", inserted)


if __name__ == "__main__":
    run_pipeline()
