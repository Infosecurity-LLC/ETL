from sqlalchemy.orm import Session

from full_incidents.thehive_to_dwh.postgresql.models import Incident


def delete_incident(session: Session, incident_id):
    session.query(Incident).filter(Incident.inc_id == incident_id).delete()
