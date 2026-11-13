from django.db import models


class Vessel(models.Model):
    name = models.CharField(max_length=150, help_text="Vessel name e.g., CAPE G")
    imo = models.CharField(max_length=50, blank=True, null=True, db_index=True, help_text="IMO number")
    mmsi = models.CharField(max_length=50, blank=True, null=True, db_index=True, help_text="MMSI number")

    call_sign = models.CharField(max_length=50, blank=True, null=True, help_text="Radio call sign")
    ship_type = models.CharField(max_length=100, blank=True, null=True, help_text="Type of ship")
    ais_destination = models.CharField(
        max_length=100, blank=True, null=True,
        help_text="Destination as reported by the vessel via AIS (read-only)"
    )
    destination_port = models.CharField(max_length=100, blank=True, null=True, help_text="Destination port")
    draught = models.FloatField(blank=True, null=True, help_text="Maximum static draught")

    destination_lat = models.FloatField(blank=True, null=True, help_text="Target Destination Latitude")
    destination_lon = models.FloatField(blank=True, null=True, help_text="Target Destination Longitude")

    # Live telemetry (saved by the collect_ais command)
    latitude = models.FloatField(blank=True, null=True, help_text="Current latitude")
    longitude = models.FloatField(blank=True, null=True, help_text="Current longitude")
    speed_over_ground = models.FloatField(blank=True, null=True, help_text="SOG")
    course_over_ground = models.FloatField(blank=True, null=True, help_text="COG")
    navigational_status = models.CharField(max_length=100, blank=True, null=True, help_text="Nav status")

    # Time of the last AIS position received (set by collect_ais)
    last_updated = models.DateTimeField(auto_now=True, help_text="Timestamp of last position packet")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} (MMSI: {self.mmsi})"