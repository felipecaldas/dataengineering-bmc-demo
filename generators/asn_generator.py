#!/usr/bin/env python3
"""Compatibility entry point for the supplier ASN generator."""

import argparse
from datetime import date

from demo.config import settings
from demo.simulate import generate_asn


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=settings.trading_date.isoformat())
    parser.add_argument("--lines", type=int, default=5000)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(generate_asn(date.fromisoformat(args.date), args.lines, args.force))

