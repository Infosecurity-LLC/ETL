from sqlalchemy.orm import Session

from full_incidents.hive_to_dwh.postgresql.models import Asset


def insert_asset(session: Session, org_id, fqdn=None, hostname=None, ip=None, date_seen=None,
                 is_collector=None):
    asset = Asset()
    asset.org_id = org_id
    asset.first_seen = date_seen
    asset.last_seen = date_seen
    asset.fqdn = fqdn
    asset.hostname = hostname
    asset.ip = ip
    asset.is_collector = is_collector
    session.add(asset)
    session.commit()
