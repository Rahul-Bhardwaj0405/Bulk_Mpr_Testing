from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings
import logging

from pytz import timezone

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bulk_both.settings')

app = Celery('bulk_both')
app.conf.enable_utc = False
app.conf.update(timezone = 'Asia/Kolkata')

# Use Redis as the broker
app.config_from_object('django.conf:settings', namespace='CELERY')

# Celery Beat Settings
# app.conf.beat_schedule = {

# }


# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# @app.task(bind=True)
# def debug_task(self):
#     print(f"Request: {self.request!r}")