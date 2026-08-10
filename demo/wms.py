from __future__ import annotations

import io
from datetime import date

import paramiko

from demo import blob
from demo.config import settings
from demo.gates import ack_name, order_name


def deliver_to_wms(trading_date: date) -> dict:
    source = order_name(trading_date)
    if not blob.exists(source):
        raise FileNotFoundError(f"Order blob does not exist: {source}")
    content = blob.download_bytes(source)
    filename = source.rsplit("/", 1)[-1]
    transport = paramiko.Transport((settings.wms_host, settings.wms_port))
    try:
        transport.connect(username=settings.wms_user, password=settings.wms_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        with sftp.open(f"/inbound/replen/{filename}", "wb") as handle:
            handle.write(content)
    finally:
        transport.close()
    return {"source": source, "destination": f"sftp://wms/inbound/replen/{filename}", "bytes": len(content)}


def ack_exists(trading_date: date) -> bool:
    transport = paramiko.Transport((settings.wms_host, settings.wms_port))
    try:
        transport.connect(username=settings.wms_user, password=settings.wms_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            sftp.stat(f"/ack/{ack_name(trading_date)}")
            return True
        except FileNotFoundError:
            return False
    finally:
        transport.close()

