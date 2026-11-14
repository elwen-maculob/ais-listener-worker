"""
Short-burst AIS collector for the P-NKKK portal.

Listens to AISStream for a limited time, reconnects if the socket drops,
saves the latest data for the vessels on the schedule board, then exits.

Save as: TrackerApp/management/commands/collect_ais.py
"""
import asyncio
import json
import time

import websockets
from websockets.exceptions import WebSocketException

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.utils import timezone

from schedules.models import ActiveSchedule
from TrackerApp.models import Vessel

URI = "wss://stream.aisstream.io/v0/stream"
MAX_MMSI_FILTER = 50

RECV_TIMEOUT = 30    # seconds without a message is normal for quiet vessels: keep the connection open
MAX_RETRIES = 5      # give up after this many failed connection attempts in a row
RETRY_DELAY = 2      # seconds; multiplied by the attempt number, capped at 10

CLIENT_PORTS = {
    "PSC": ("PSC PORT VILLANUEVA", 8.5782, 124.7581),
    "PKI": ("PKI PORT JASAAN", 8.6363, 124.7603),
    "PMSC_BOHOL": ("PMSC PORT BOHOL", 9.6169, 124.3094),
    "PMSC_ALCOY": ("PMSC PORT ALCOY", 9.7350, 123.5144),
}

NAV_STATUS = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuvrability",
    4: "Constrained by draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    11: "Towing astern",
    12: "Pushing ahead / towing alongside",
    14: "AIS-SART active",
    15: "Not defined",
}


def digits_only(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def clean_position(body):
    fields = {}
    lat, lon = body.get("Latitude"), body.get("Longitude")
    if lat is not None and lon is not None and abs(lat) <= 90 and abs(lon) <= 180:
        fields["latitude"] = lat
        fields["longitude"] = lon

    sog = body.get("Sog")
    if sog is not None and sog < 102.3:
        fields["speed_over_ground"] = sog

    cog = body.get("Cog")
    if cog is not None and cog < 360:
        fields["course_over_ground"] = cog

    nav = body.get("NavigationalStatus")
    if nav in NAV_STATUS:
        fields["navigational_status"] = NAV_STATUS[nav]

    return fields


def clean_static(body):
    fields = {}
    call_sign = (body.get("CallSign") or "").replace("@", "").strip()
    if call_sign:
        fields["call_sign"] = call_sign

    if body.get("Type"):
        fields["ship_type"] = str(body["Type"])

    destination = (body.get("Destination") or "").replace("@", "").strip()
    if destination:
        fields["ais_destination"] = destination

    if body.get("MaximumStaticDraught"):
        fields["draught"] = body["MaximumStaticDraught"]

    return fields


def sync_vessels_from_schedule():
    wanted = {}
    skipped = 0
    rows = (
        ActiveSchedule.objects
        .exclude(mmsi__isnull=True).exclude(mmsi="")
        .order_by("eta", "id")
    )
    for row in rows:
        mmsi = digits_only(row.mmsi)
        if len(mmsi) != 9:
            skipped += 1
            continue
        wanted.setdefault(mmsi, row)

    existing = {v.mmsi: v for v in Vessel.objects.filter(mmsi__in=list(wanted))}

    for mmsi, row in wanted.items():
        changes = {
            "name": " ".join((row.vessel_name or "").split()).upper(),
            "imo": digits_only(row.imo) or None,
        }
        port = CLIENT_PORTS.get(row.client)
        if port and port[0] and port[1] is not None and port[2] is not None:
            changes.update(destination_port=port[0], destination_lat=port[1], destination_lon=port[2])

        vessel = existing.get(mmsi)
        if vessel is None:
            Vessel.objects.create(mmsi=mmsi, **changes)
        else:
            diff = {k: v for k, v in changes.items() if getattr(vessel, k) != v}
            if diff:
                Vessel.objects.filter(pk=vessel.pk).update(**diff)

    return list(wanted), skipped


async def collect_with_retries(api_key, mmsis, max_seconds, want_static):
    """
    Listen to AISStream until every vessel has reported or the time runs out.
    If the socket drops, reconnect and keep going until the deadline.
    """
    targets = set(mmsis)
    positions, statics = {}, {}
    deadline = time.monotonic() + max_seconds
    failures = 0

    payload = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": list(mmsis),
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    while time.monotonic() < deadline:
        try:
            async with websockets.connect(URI, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(payload))

                while time.monotonic() < deadline:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=max(0.05, min(RECV_TIMEOUT, deadline - time.monotonic())),
                        )
                    except asyncio.TimeoutError:
                        # Silence is normal for quiet vessels. Keep the SAME connection open:
                        # closing and reopening it here would only lose messages.
                        continue

                    failures = 0  # the connection works, so reset the failure counter
                    message = json.loads(raw)

                    if "error" in message:
                        # For example an invalid API key: stop with a visible failure
                        # instead of reconnecting until the time is up.
                        raise CommandError(f"AISStream returned an error: {message['error']}")

                    msg_type = message.get("MessageType")
                    mmsi = str(message.get("MetaData", {}).get("MMSI", ""))
                    if mmsi not in targets:
                        continue

                    body = message.get("Message", {}).get(msg_type, {})

                    if msg_type == "PositionReport":
                        positions[mmsi] = clean_position(body)
                    elif msg_type == "ShipStaticData":
                        statics[mmsi] = clean_static(body)

                    if targets <= set(positions) and (not want_static or targets <= set(statics)):
                        return positions, statics

        except (OSError, WebSocketException):
            # The connection dropped or could not be opened (this includes ConnectionClosed).
            failures += 1
            if failures >= MAX_RETRIES:
                raise CommandError(f"Could not keep a connection to AISStream ({failures} attempts failed).")
            await asyncio.sleep(min(RETRY_DELAY * failures, 10))

    return positions, statics


class Command(BaseCommand):
    help = "Listens to AISStream for a short window and saves the latest data for schedule vessels."

    def add_arguments(self, parser):
        parser.add_argument("--max-seconds", type=int, default=300)
        parser.add_argument("--statics", action="store_true")

    def handle(self, *args, **options):
        api_key = getattr(settings, "AISSTREAM_API_KEY", None)
        if not api_key:
            raise CommandError("AISSTREAM_API_KEY is not set.")

        mmsis, skipped = sync_vessels_from_schedule()
        if skipped:
            self.stdout.write(
                self.style.WARNING(f"{skipped} schedule row(s) skipped: the MMSI is not 9 digits.")
            )
        if not mmsis:
            self.stdout.write("No vessels with a valid MMSI on the schedule board.")
            return

        if len(mmsis) > MAX_MMSI_FILTER:
            mmsis = mmsis[:MAX_MMSI_FILTER]

        connections.close_all()

        try:
            positions, statics = asyncio.run(
                collect_with_retries(api_key, mmsis, options["max_seconds"], options["statics"])
            )
        except CommandError:
            raise
        except Exception as exc:
            raise CommandError(f"AIS collection failed: {exc}")

        now = timezone.now()
        saved = 0
        for mmsi in set(positions) | set(statics):
            fields = {**statics.get(mmsi, {}), **positions.get(mmsi, {})}
            if mmsi in positions:
                fields["last_updated"] = now

            if fields:
                Vessel.objects.filter(mmsi=mmsi).update(**fields)
                saved += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Targeted {len(mmsis)} vessels: {len(positions)} reported positions, "
                f"{len(statics)} sent static data, {saved} updated in DB."
            )
        )