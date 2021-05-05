import logging

from sqlalchemy.orm import Session

import datetime

from full_incidents.hive_to_dwh.postgresql.insert import insert_asset
from full_incidents.hive_to_dwh.postgresql.models import Asset

logger = logging.getLogger('hive_to_dwh')


def load(assets, sys_date, session: Session):
    inserted_assets_number = 0
    updated_assets_number = 0
    date_seen = datetime.date(int(sys_date["sys_year"]), int(sys_date["sys_month"]), int(sys_date["sys_day"]))
    try:
        for asset in assets:
            if asset["is_source"] is True and asset["isnetworklocal"] is False:
                asset["organization"] = None
            asset_id = session.query(Asset.id).filter(Asset.org_id == asset["organization"],
                                                      Asset.hostname == asset["hostname"],
                                                      Asset.ip == asset["ip"]).first()
            if asset_id:
                session.query(Asset).filter(Asset.id == asset_id).update({"last_seen": date_seen})
                updated_assets_number += 1

            if not asset_id:
                insert_asset(fqdn=asset["fqdn"],
                             hostname=asset["hostname"],
                             ip=asset["ip"],
                             date_seen=date_seen,
                             org_id=asset["organization"],
                             is_collector=asset["is_collector"],
                             session=session)
                inserted_assets_number += 1

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        logging.info("Inserted assets number: %d", inserted_assets_number)
        logging.info("Updated assets number: %d", updated_assets_number)
