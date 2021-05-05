import logging
from typing import List, Dict

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from full_incidents.translator_to_dwh.load.models import Categories, Languages, Usecase, Rule

logger = logging.getLogger('translator_to_dwh')


def load(usecases: List[Dict], rules, categories, session: Session):
    insert_categories_number = 0
    update_categories_number = 0
    usecases_number = 0
    rules_number = 0
    try:
        for category in categories:
            select_category = session.query(Categories).filter(Categories.category_id == category.category_id,
                                                               Categories.lang_id == category.lang_id).first()
            if not select_category:
                insert_category = Categories()
                insert_category.category_id = category.category_id
                insert_category.lang_id = category.lang_id
                insert_category.description = category.description
                session.add(insert_category)
                session.flush()
                insert_categories_number += 1

            elif select_category.description != category.description:
                session.query(Categories).filter(Categories.id == select_category.id).update(
                    {"description": category.description})
                update_categories_number += 1

        for usecase in usecases:
            language_id = session.query(Languages.id).filter(Languages.locale == usecase["lang_id"]).first()
            usecase_insert_object = insert(table=Usecase).values(
                id=usecase["id"],
                lang_id=language_id,
                usecase_id=usecase["usecase_id"],
                description=usecase["description"],
                category_id=usecase["category_id"]
            )
            usecase_upsert_object = usecase_insert_object.on_conflict_do_update(index_elements=['id'], set_=dict(
                lang_id=language_id,
                usecase_id=usecase["usecase_id"],
                description=usecase["description"],
                category_id=usecase["category_id"]
            ))
            session.execute(usecase_upsert_object)
            usecases_number += 1

        for rule in rules:
            language_id = session.query(Languages).filter(Languages.locale == rule.lang_id).first().id

            rule_insert_object = insert(table=Rule).values(
                id=rule.id,
                lang_id=language_id,
                rule_name=rule.rule_name,
                description=rule.description
            )
            rule_upsert_object = rule_insert_object.on_conflict_do_update(index_elements=['id'], set_=dict(
                lang_id=language_id,
                rule_name=rule.rule_name,
                description=rule.description
            ))
            session.execute(rule_upsert_object)
            rules_number += 1
        session.commit()
    except Exception:
        session.rollback()
        raise Exception
    finally:
        session.close()
        logging.info("Inserted categories number: %d", insert_categories_number)
        logging.info("Updated categories number: %d", update_categories_number)
        logging.info("Inserted and updated usecases number: %d", usecases_number)
        logging.info("Inserted and updated rules number: %d", rules_number)
