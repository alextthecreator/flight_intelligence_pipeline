from __future__ import annotations

import logging
import json
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
from typing import Any
from urllib.parse import quote_plus, urlencode

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
ALERT_TREND_HISTORY_DAYS = 7
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


def get_alert_drop_threshold_pct() -> Decimal:
    configured = os.getenv("ALERT_DROP_THRESHOLD_PCT", "").strip()
    if not configured:
        return ALERT_DROP_THRESHOLD_PCT

    try:
        return Decimal(configured)
    except (InvalidOperation, ValueError):
        logging.warning(
            "Invalid ALERT_DROP_THRESHOLD_PCT value '%s'. Falling back to %s.",
            configured,
            ALERT_DROP_THRESHOLD_PCT,
        )
        return ALERT_DROP_THRESHOLD_PCT


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
        booking_link = str(offer.get("booking_url") or "").strip()
        if not booking_link.startswith(("http://", "https://")):
            booking_link = "https://www.google.com/travel/flights"

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


def fetch_recent_route_records(
    supabase: Client, route: str, days: int = ALERT_TREND_HISTORY_DAYS
) -> list[dict[str, Any]]:
    start_date = (date.today() - timedelta(days=days - 1)).isoformat()
    try:
        response = (
            supabase.table("flight_prices")
            .select("created_at, simulated_price")
            .eq("route", route)
            .gte("created_at", start_date)
            .order("created_at", desc=False)
            .execute()
        )
        return response.data or []
    except Exception as exc:  # noqa: BLE001 - preserving DB errors in logs
        logging.error("Supabase recent history query failed for route %s: %s", route, exc)
        return []


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


def build_alert_trend_series(
    history_records: list[dict[str, Any]], current_price: Decimal
) -> tuple[list[str], list[float]]:
    daily_prices: dict[str, Decimal] = {}
    for item in history_records:
        created_at_raw = item.get("created_at")
        price_raw = item.get("simulated_price")
        if created_at_raw is None or price_raw is None:
            continue

        try:
            created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
            price = Decimal(str(price_raw))
        except (ValueError, TypeError, InvalidOperation):
            continue

        day_key = created_at.date().isoformat()
        daily_prices[day_key] = price

    sorted_days = sorted(daily_prices.items())
    history_points = sorted_days[-ALERT_TREND_HISTORY_DAYS :]

    labels = [day[0][5:] for day in history_points]
    values = [float(day[1]) for day in history_points]

    today_label = date.today().isoformat()[5:]
    if labels and labels[-1] == today_label:
        labels[-1] = "Today"
        values[-1] = float(current_price)
    else:
        labels.append("Today")
        values.append(float(current_price))
    return labels, values


def build_quickchart_url(
    old_price: Decimal,
    new_price: Decimal,
    currency: str,
    labels: list[str] | None = None,
    values: list[float] | None = None,
) -> str:
    chart_labels = labels if labels else ["Previous", "Current"]
    chart_values = values if values else [float(old_price), float(new_price)]
    highest_visible_price = max(chart_values) if chart_values else float(old_price)
    y_axis_target = Decimal(str(highest_visible_price)) + Decimal("50")
    y_axis_max = float(
        (y_axis_target / Decimal("50")).to_integral_value(rounding=ROUND_CEILING) * Decimal("50")
    )
    chart_config = {
        "version": "4",
        "type": "line",
        "data": {
            "labels": chart_labels,
            "datasets": [
                {
                    "label": f"Price ({currency})",
                    "data": chart_values,
                    "borderColor": "#ef4444",
                    "backgroundColor": "rgba(239,68,68,0.15)",
                    "fill": True,
                    "tension": 0.35,
                    "pointRadius": 4,
                }
            ],
        },
        "options": {
            "plugins": {"legend": {"display": False}},
            "scales": {
                # Force showing all day labels on the x-axis.
                "x": {"ticks": {"autoSkip": False, "maxRotation": 0, "minRotation": 0}},
                # `y` is used by Chart.js v3/v4.
                "y": {"min": 0, "max": y_axis_max, "beginAtZero": True},
                # `xAxes`/`yAxes` keep compatibility for engines interpreting v2 syntax.
                "xAxes": [{"ticks": {"autoSkip": False, "maxRotation": 0, "minRotation": 0}}],
                # `yAxes` keeps compatibility with environments still interpreting v2 syntax.
                "yAxes": [{"ticks": {"min": 0, "max": y_axis_max, "beginAtZero": True}}],
            },
        },
    }
    encoded_config = quote_plus(json.dumps(chart_config, separators=(",", ":")))
    return f"https://quickchart.io/chart?c={encoded_config}"


def build_alert_booking_link(origin: str, destination: str, departure_date: date, booking_link: str) -> str:
    if booking_link.startswith(("http://", "https://")) and booking_link != "https://www.google.com/travel/flights":
        return booking_link

    query_params = {
        "q": f"Flights from {origin} to {destination} on {departure_date.isoformat()}",
    }
    return f"https://www.google.com/travel/flights?{urlencode(query_params)}"


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
    departure_date: date,
    old_price: Decimal,
    new_price: Decimal,
    currency: str,
    drop_pct: Decimal,
    booking_link: str,
    trend_labels: list[str] | None = None,
    trend_values: list[float] | None = None,
) -> bool:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    sender_email = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev").strip()
    recipient_email = os.getenv("ALERT_EMAIL_TO", "").strip()

    if not api_key:
        logging.warning("RESEND_API_KEY is missing. Skipping alert email.")
        return False
    if not recipient_email:
        logging.warning("ALERT_EMAIL_TO is missing. Skipping alert email.")
        return False

    chart_url = build_quickchart_url(
        old_price=old_price,
        new_price=new_price,
        currency=currency,
        labels=trend_labels,
        values=trend_values,
    )
    alert_booking_link = build_alert_booking_link(
        origin=route_from,
        destination=route_to,
        departure_date=departure_date,
        booking_link=booking_link,
    )
    html_body = render_drop_alert_html(
        route_from=route_from,
        route_to=route_to,
        old_price=old_price,
        new_price=new_price,
        currency=currency,
        drop_pct=drop_pct,
        chart_url=chart_url,
        booking_link=alert_booking_link,
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
        return True
    except Exception as exc:  # noqa: BLE001 - keep pipeline running on alert failures
        logging.error("Failed to send email alert for route %s-%s: %s", route_from, route_to, exc)
        return False


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
    alerts_sent = 0
    alert_drop_threshold = get_alert_drop_threshold_pct()
    logging.info("Alert drop threshold is set to %s%%", alert_drop_threshold)

    for raw_offer in offers:
        previous = fetch_last_route_record(supabase, raw_offer.route)
        record = transform_offer(raw_offer, previous)
        if not record:
            continue

        previous_price_raw = previous.get("simulated_price") if previous else None
        previous_price = Decimal(str(previous_price_raw)) if previous_price_raw is not None else None
        abs_drop_pct = abs(record.price_change_pct)
        significant_drop = (
            previous_price is not None
            and record.trend == "DOWN"
            and abs_drop_pct > alert_drop_threshold
        )

        logging.info(
            "Alert evaluation for %s | previous=%s current=%s trend=%s drop_pct=%s threshold=%s trigger=%s",
            record.route,
            previous_price if previous_price is not None else "n/a",
            record.simulated_price,
            record.trend,
            abs_drop_pct,
            alert_drop_threshold,
            significant_drop,
        )

        if significant_drop:
            recent_history = fetch_recent_route_records(supabase=supabase, route=record.route)
            trend_labels, trend_values = build_alert_trend_series(
                history_records=recent_history,
                current_price=record.simulated_price,
            )
            email_sent = send_email_alert(
                route_from=record.origin,
                route_to=record.destination,
                departure_date=record.departure_date,
                old_price=previous_price,
                new_price=record.simulated_price,
                currency=record.currency,
                drop_pct=abs(record.price_change_pct),
                booking_link=record.booking_link,
                trend_labels=trend_labels,
                trend_values=trend_values,
            )
            if email_sent:
                alerts_sent += 1

        if load_record(supabase, record):
            inserted += 1

    logging.info("Pipeline completed. Inserted rows: %s, alerts sent: %s", inserted, alerts_sent)


if __name__ == "__main__":
    run_pipeline()
