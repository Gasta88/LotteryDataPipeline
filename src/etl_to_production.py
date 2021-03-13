#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to move records from staging to production tables."""

import logging
import sqlite3
import pandas as pd
import numpy as np

logger = logging.getLogger('file_logger')

def _get_last_event(db_file):
    """Get audit time from the last processing into staging area (PRIVATE)."""
    with sqlite3.connect(db_file) as conn:
        sql = "SELECT last_event FROM audit_events;"
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

def prepare_discount_table(db_file):
    