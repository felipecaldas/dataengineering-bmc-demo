#!/usr/bin/env python3
"""Compatibility entry point for the store simulator."""

import argparse
from datetime import date

from demo.config import settings
from demo.simulate import simulate_day


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=settings.trading_date.isoformat())
    parser.add_argument("--withhold", type=int)
    args = parser.parse_args()
    print(simulate_day(date.fromisoformat(args.date), args.withhold))

