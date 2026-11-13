from django.db import models

class ActiveSchedule(models.Model):
    CLIENT_CHOICES = [
        ('PSC', 'PSC'),
        ('PKI', 'PKI'),
        ('PMSC_BOHOL', 'PMSC Bohol'),
        ('PMSC_ALCOY', 'PMSC Alcoy'),
        ('OTHER', 'Other / Special Client (Gracia, THPAL, etc.)'),
    ]

    client = models.CharField(max_length=50, choices=CLIENT_CHOICES, default='PSC')
    custom_client_name = models.CharField(max_length=100, blank=True, null=True, help_text="Used if 'Other' is selected")
    
    vessel_name = models.CharField(max_length=150)
    imo = models.CharField(max_length=20, blank=True, null=True)
    mmsi = models.CharField(max_length=20, blank=True, null=True)
    cargo = models.CharField(max_length=150, blank=True, null=True)
    quantity = models.CharField(max_length=100, blank=True, null=True)
    eta = models.DateField(help_text="Estimated Time of Arrival", blank=True, null=True)
    surveyor_in_charge = models.CharField(max_length=100, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['eta', 'id']  # Now sorts chronologically by date!

    def __str__(self):
        client_display = self.custom_client_name if self.client == 'OTHER' and self.custom_client_name else self.get_client_display()
        return f"[{client_display}] {self.vessel_name} (ETA: {self.eta})"