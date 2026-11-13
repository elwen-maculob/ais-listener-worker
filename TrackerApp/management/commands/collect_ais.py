"""
Short-burst AIS collector for the P-NKKK portal.

1. Reads the vessels on the schedule board (ActiveSchedule) that have a valid MMSI.
2. Makes sure each one has a row in TrackerApp.Vessel (created or refreshed).
3. Listens to AISStream until every vessel has reported, or the time limit is hit.
4. Saves the latest data in one batch, then exits.

Run it on a schedule (GitHub Actions) instead of keeping a worker online 24/7.

Save as: TrackerApp/management/commands/collect_ais.py

Usage:
    python manage.py collect_ais                      # positions only, up to 300 s
    python manage.py collect_ais --statics --max-seconds 420
"""
import asyncio
import json
import time

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.utils import timezone

from schedules.models import ActiveSchedule
from TrackerApp.models import Vessel

URI = "wss://stream.aisstream.io/v0/stream"
MAX_MMSI_FILTER = 50  # AISStream accepts at most 50 MMSIs per subscription

# Planned destination for each client group on the schedule board:
#   "CLIENT_CODE": ("port name", latitude, longitude)
# Fill in the coordinates (decimal degrees) for the ports you serve.
# Entries left as None are skipped, so you can still set a destination by hand.
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
    """Convert an AIS PositionReport into model fields, skipping 'not available' values."""
    fields = {}

    lat, lon = body.get("Latitude"), body.get("Longitude")
    if lat is not None and lon is not None and abs(lat) <= 90 and abs(lon) <= 180:
        fields["latitude"] = lat
        fields["longitude"] = lon

    sog = body.get("Sog")
    if sog is not None and sog < 102.3:  # 102.3 = speed not available
        fields["speed_over_ground"] = sog

    cog = body.get("Cog")
    if cog is not None and cog < 360:  # 360 = course not available
        fields["course_over_ground"] = cog

    nav = body.get("NavigationalStatus")
    if nav in NAV_STATUS:
        fields["navigational_status"] = NAV_STATUS[nav]

    return fields


def clean_static(body):
    """Convert AIS ShipStaticData into model fields."""
    fields = {}

    call_sign = (body.get("CallSign") or "").replace("@", "").strip()
    if call_sign:
        fields["call_sign"] = call_sign

    if body.get("Type"):
        fields["ship_type"] = str(body["Type"])

    # The crew's typed-in destination goes to ais_destination only,
    # so destination_port (your planned port) is never overwritten by it.
    destination = (body.get("Destination") or "").replace("@", "").strip()
    if destination:
        fields["ais_destination"] = destination

    if body.get("MaximumStaticDraught"):
        fields["draught"] = body["MaximumStaticDraught"]

    return fields


def sync_vessels_from_schedule():
    """
    Make sure every vessel on the schedule board that has a valid MMSI has a
    Vessel row. Returns (list of MMSIs to track, number of rows skipped).
    """
    wanted = {}
    skipped = 0
    rows = (
        ActiveSchedule.objects
        .exclude(mmsi__isnull=True).exclude(mmsi="")
        .order_by("eta", "id")
    )
    for row in rows:
        mmsi = digits_only(row.mmsi)
        if len(mmsi) != 9:  # an MMSI always has exactly 9 digits
            skipped += 1
            continue
        wanted.setdefault(mmsi, row)  # keep the first row (earliest ETA) per vessel

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
                # update() leaves last_updated (the time of the last position) untouched
                Vessel.objects.filter(pk=vessel.pk).update(**diff)

    return list(wanted), skipped


async def collect(api_key, mmsis, max_seconds, want_static):
    """Listen until all tracked vessels have reported, or the time limit is hit."""
    targets = set(mmsis)
    positions, statics = {}, {}
    deadline = time.monotonic() + max_seconds

    payload = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": list(mmsis),
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    async with websockets.connect(URI, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(payload))

        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=max(0.1, min(30, deadline - time.monotonic()))
                )
            except asyncio.TimeoutError:
                continue
            except ConnectionClosed:
                break

            message = json.loads(raw)

            if "error" in message:
                raise CommandError(f"AISStream returned an error: {message['error']}")

            msg_type = message.get("MessageType")
            mmsi = str(message.get("MetaData", {}).get("MMSI", ""))
            if mmsi not in targets:
                continue

            body = message.get("Message", {}).get(msg_type, {})

            if msg_type == "PositionReport":
                positions[mmsi] = clean_position(body)  # later reports replace earlier ones
            elif msg_type == "ShipStaticData":
                statics[mmsi] = clean_static(body)

            all_positions = targets <= set(positions)
            all_statics = targets <= set(statics)
            if all_positions and (not want_static or all_statics):
                break

    return positions, statics


class Command(BaseCommand):
    help = "Listens to AISStream for a short window and saves the latest data for the vessels on the schedule board."

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-seconds",
            type=int,
            default=300,
            help="Stop listening after this many seconds (default: 300).",
        )
        parser.add_argument(
            "--statics",
            action="store_true",
            help="Also wait for ship static data (destination, call sign, type, draught).",
        )

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
            self.stdout.write(
                self.style.WARNING(
                    f"{len(mmsis)} vessels found; only the first {MAX_MMSI_FILTER} can be tracked."
                )
            )
            mmsis = mmsis[:MAX_MMSI_FILTER]

        # Release the database connection while waiting, so a serverless
        # database (like Neon) is not kept awake for the whole listening window.
        connections.close_all()

        try:
            positions, statics = asyncio.run(
                collect(api_key, mmsis, options["max_seconds"], options["statics"])
            )
        except (OSError, WebSocketException) as exc:
            raise CommandError(f"Could not reach AISStream: {exc}")

        now = timezone.now()
        saved = 0
        for mmsi in set(positions) | set(statics):
            fields = {**statics.get(mmsi, {}), **positions.get(mmsi, {})}
            if mmsi in positions:
                # last_updated means "time of the last position": only set it for position reports
                fields["last_updated"] = now

            if fields:
                Vessel.objects.filter(mmsi=mmsi).update(**fields)
                saved += 1

        # Keep the output free of vessel names, MMSIs and positions.
        self.stdout.write(
            self.style.SUCCESS(
                f"{len(positions)} of {len(mmsis)} vessels reported a position; "
                f"{len(statics)} sent static data; {saved} records saved."
            )
        )