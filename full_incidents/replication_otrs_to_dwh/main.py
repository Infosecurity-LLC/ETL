import pymysql
import psycopg2
import logging
from airflow.hooks.base_hook import BaseHook

from dateutil.parser import parse
from datetime import timezone, timedelta

from exceptions import ErrorMySqlParameters, ErrorStartDateParser, ErrorPostgreParameters
from full_incidents.replication_otrs_to_dwh.postgresql.upload import UploadIntoTicketStateType, UploadIntoTicketState, \
    UploadIntoTicketType, \
    UploadIntoTicketPriority, UploadIntoTicketLockType, UploadIntoQueue, UploadIntoService, UploadIntoSla, \
    UploadIntoUsers, UploadIntoOrganizations, UploadIntoCustomerUser, UploadIntoTicketHistoryType, UploadIntoTicket, \
    UploadIntoTicketHistory, UploadClosureCode, UploadTicketCode, UploadArticle, UploadArticleDataMime

logger = logging.getLogger('replication_otrs_to_dwh')


def connect_to_mysql(mysql_args):
    try:
        host = mysql_args.host
        user = mysql_args.login
        password = mysql_args.password
        db = mysql_args.schema
    except KeyError as e:
        raise ErrorMySqlParameters(f"No needed parameters for mysql connection: {e}")
    mysql_conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db
    )
    return mysql_conn


def connect_to_pg(postgre_args):
    try:
        host = postgre_args.host
        database = postgre_args.schema
        user = postgre_args.login
        password = postgre_args.password
    except KeyError as e:
        raise ErrorPostgreParameters(f"No needed parameters for postgre connection: {e}")
    conn = psycopg2.connect(host=host,
                            database=database,
                            user=user,
                            password=password)
    return conn


def pars_start_time(t: str):
    try:
        start_date_time = parse(t)
    except Exception as err:
        raise ErrorStartDateParser(f"Error in start date parser: {err}")
    return start_date_time


class GetData:
    def __init__(self, mysql_conn, mysql_cur, execution_start_date):
        self.mysql_conn = mysql_conn
        self.mysql_cur = mysql_cur
        self.extraction_start_date = execution_start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        # 24 часа + 30 мин следующих суток, чтобы не потерять тикеты, которые создались после 00:00
        self.extraction_end_date = (execution_start_date + timedelta(hours=24, minutes=30)).\
            strftime("%Y-%m-%d %H:%M:%S.%f")

    def get_data_from_table(self, table: str, items: list, query: str = None, is_logging_enabled: str = True) -> list:
        if not query:
            columns = items_to_str(items)
            query = f"""
                select {columns}
                from {table}
                where change_time >= '{self.extraction_start_date}'
                and change_time < '{self.extraction_end_date}'
            """
        self.mysql_cur.execute(query)
        rows = self.mysql_cur.fetchall()

        data_from_table = []
        for row in rows:
            row_dict = dict(zip(items, row))
            data_from_table.append(row_dict)
        if is_logging_enabled:
            logger.info("Select %d rows for table %s where change_time >= %s and change_time < %s", len(data_from_table),
                        table, self.extraction_start_date, self.extraction_end_date)
        return data_from_table


def items_to_str(items):
    return ', '.join(items)


def ticket_state_type(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_state_type")
    fields = ['id', 'name']
    fields_str = items_to_str(fields)

    upl_ticket_state_type = UploadIntoTicketStateType(pg_conn, pg_cur)
    is_table_empty = upl_ticket_state_type.is_table_empty('ticket_state_type')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from ticket_state_type
        """
        data = get_data.get_data_from_table(items=fields, table='ticket_state_type', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='ticket_state_type')
    upl_ticket_state_type.upload(data)


def ticket_state(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_state")
    fields = ['id', 'name', 'type_id']
    fields_str = items_to_str(fields)

    upl_ticket_state = UploadIntoTicketState(pg_conn, pg_cur)
    is_table_empty = upl_ticket_state.is_table_empty('ticket_state')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from ticket_state
        """
        data = get_data.get_data_from_table(items=fields, table='ticket_state', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='ticket_state')
    upl_ticket_state.upload(data)


def ticket_type(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_type")
    fields = ['id', 'name']
    fields_str = items_to_str(fields)

    upl_ticket_type = UploadIntoTicketType(pg_conn, pg_cur)
    is_table_empty = upl_ticket_type.is_table_empty('ticket_type')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from ticket_type
        """
        data = get_data.get_data_from_table(items=fields, table='ticket_type', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='ticket_type')
    upl_ticket_type.upload(data)


def ticket_priority(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_priority")
    data = get_data.get_data_from_table(items=['id', 'name'], table='ticket_priority')
    upl_ticket_priority = UploadIntoTicketPriority(pg_conn, pg_cur)
    upl_ticket_priority.upload(data)


def ticket_lock_type(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_lock_type")
    fields = ['id', 'name']
    fields_str = items_to_str(fields)

    upl_ticket_lock_type = UploadIntoTicketLockType(pg_conn, pg_cur)
    is_table_empty = upl_ticket_lock_type.is_table_empty('ticket_lock_type')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from ticket_lock_type
        """
        data = get_data.get_data_from_table(items=fields, table='ticket_lock_type', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='ticket_lock_type')
    upl_ticket_lock_type.upload(data)


def queue(pg_conn, pg_cur, get_data):
    logger.info("Start replication queue")
    fields = ['id', 'name']
    fields_str = items_to_str(fields)

    upl_queue = UploadIntoQueue(pg_conn, pg_cur)
    is_table_empty = upl_queue.is_table_empty('queue')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from queue
        """
        data = get_data.get_data_from_table(items=fields, table='queue', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='queue')
    upl_queue.upload(data)


def service(pg_conn, pg_cur, get_data):
    logger.info("Start replication service")
    fields = ['id', 'name', 'criticality']
    fields_str = items_to_str(fields)

    upl_service = UploadIntoService(pg_conn, pg_cur)
    is_table_empty = upl_service.is_table_empty('service')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from service
        """
        data = get_data.get_data_from_table(items=fields, table='service', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='service')
    upl_service.upload(data)


def sla(pg_conn, pg_cur, get_data):
    logger.info("Start replication sla")
    fields = ['id', 'name', 'first_response_time', 'first_response_notify',
              'update_time', 'update_notify', 'solution_time', 'solution_notify']
    fields_str = items_to_str(fields)

    upl_sla = UploadIntoSla(pg_conn, pg_cur)
    is_table_empty = upl_sla.is_table_empty('sla')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from sla
        """
        data = get_data.get_data_from_table(items=fields, table='sla', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='sla')
    upl_sla.upload(data)


def users(pg_conn, pg_cur, get_data):
    logger.info("Start replication users")
    fields = ['id', 'login', 'first_name', 'last_name']
    fields_str = items_to_str(fields)

    upl_users = UploadIntoUsers(pg_conn, pg_cur)
    is_table_empty = upl_users.is_table_empty('users')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from users
        """
        data = get_data.get_data_from_table(items=fields, table='users', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='users')
    upl_users.upload(data)


def customer_company(pg_conn, pg_cur, get_data):
    logger.info("Start replication customer_company")
    data = get_data.get_data_from_table(items=['customer_id', 'name'], table='customer_company')
    upl_org = UploadIntoOrganizations(pg_conn, pg_cur)
    upl_org.upload(data)


def customer_user(pg_conn, pg_cur, get_data):
    logger.info("Start replication sla")
    fields = ['login', 'email', 'first_name', 'last_name', 'phone', 'mobile', 'id', 'customer_id']
    data = get_data.get_data_from_table(items=fields, table='customer_user')
    upl_cust_user = UploadIntoCustomerUser(pg_conn, pg_cur)
    upl_cust_user.upload(data)


def ticket_history_type(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_history_type")
    fields = ['id', 'name']
    fields_str = items_to_str(fields)

    upl_ticket_history_type = UploadIntoTicketHistoryType(pg_conn, pg_cur)
    is_table_empty = upl_ticket_history_type.is_table_empty('ticket_history_type')
    if is_table_empty:
        query = f"""
        select {fields_str}
        from ticket_history_type
        """
        data = get_data.get_data_from_table(items=fields, table='ticket_history_type', query=query)
    else:
        data = get_data.get_data_from_table(items=fields, table='ticket_history_type')
    upl_ticket_history_type.upload(data)


def ticket(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket")
    fields = ['title', 'queue_id', 'ticket_lock_id', 'type_id', 'service_id', 'sla_id', 'ticket_priority_id',
              'ticket_state_id', 'customer_id', 'timeout', 'until_time', 'escalation_time', 'escalation_update_time',
              'escalation_response_time', 'escalation_solution_time', 'id', 'tn', 'create_time', 'change_time',
              'customer_user_id']
    data = get_data.get_data_from_table(items=fields, table='ticket')
    upl_ticket = UploadIntoTicket(pg_conn, pg_cur)
    upl_ticket.upload(data)
    return data


def ticket_history(pg_conn, pg_cur, get_data):
    logger.info("Start replication ticket_history")
    fields = ['id', 'ticket_id', 'name', 'history_type_id', 'queue_id', 'owner_id', 'create_time', 'change_time']
    data = get_data.get_data_from_table(items=fields, table='ticket_history')
    upl_ticket_history = UploadIntoTicketHistory(pg_conn, pg_cur)
    upl_ticket_history.upload(data)


def closure_codes(pg_conn, pg_cur, get_data, data_with_tickets):
    logger.info("Start replication closure_codes")

    def insert_and_update_code_name():
        logger.info("Start replication closure_codes.code_name")

        fields = ['id', 'name', 'valid_id', 'comments', 'create_time', 'create_by', 'change_time', 'change_by',
                  'type_id']
        fields_str = items_to_str(fields)

        upl_queue = UploadClosureCode(pg_conn, pg_cur)
        is_table_empty = upl_queue.is_table_empty('closure_code')
        if is_table_empty:
            query = f"""
            select {fields_str}
            from RS_closure_code
            """
            data = get_data.get_data_from_table(items=fields, table='RS_closure_code', query=query)
        else:
            data = get_data.get_data_from_table(items=fields, table='RS_closure_code')
        upl_queue.upload(data)

    def insert_ticket_code(ticket_id: int):
        data = get_data.get_data_from_table(
            items=['id', 'ticket_id', 'code_id'], is_logging_enabled=False,
            table='dynamic_field_value', query=f"""
            select id, object_id as ticket_id, value_text as code_id
            from dynamic_field_value
            where field_id=(select id from dynamic_field where name = 'ClosureCode') 
            and object_id={ticket_id}""")

        upl_queue = UploadTicketCode(pg_conn, pg_cur)
        upl_queue.upload(data)

    insert_and_update_code_name()
    logger.info("Start replication closure_codes.ticket_code")
    for one_ticket in data_with_tickets:
        insert_ticket_code(one_ticket["id"])


def action_performed(pg_conn, pg_cur, get_data):
    logger.info("Start replication action_performed")

    def article():
        logger.info("Start replication action_performed.article")
        data = get_data.get_data_from_table(
            items=['id', 'ticket_id', 'article_sender_type_id', 'communication_channel_id', 'is_visible_for_customer',
                   'search_index_needs_rebuild', 'insert_fingerprint', 'create_time', 'create_by', 'change_time',
                   'change_by'],
            table='article')
        upl_queue = UploadArticle(pg_conn, pg_cur)
        upl_queue.upload(data)

    def article_data_mime():
        logger.info("Start replication action_performed.article_data_mime")
        data = get_data.get_data_from_table(
            items=['id', 'article_id', 'a_from', 'a_reply_to', 'a_to', 'a_cc', 'a_bcc', 'a_subject', 'a_message_id',
                   'a_message_id_md5', 'a_in_reply_to', 'a_references', 'a_content_type', 'a_body', 'incoming_time',
                   'content_path', 'create_time', 'create_by', 'change_time', 'change_by'],
            table='article_data_mime')
        upl_queue = UploadArticleDataMime(pg_conn, pg_cur)
        upl_queue.upload(data)

    article()
    article_data_mime()


def transport_root_id(data_with_tickets, mysql_cur, pg_cur, pg_conn):
    def get_root_id_from_otrs(object_id: int):
        select_root_id = """
            select value_text from dynamic_field_value
            where field_id = (select id from dynamic_field where name = 'IncidentRootId')
            and object_id = %s
        """
        mysql_cur.execute(select_root_id, object_id)
        try:
            root_id = mysql_cur.fetchone()[0]
        except TypeError:
            root_id = None
        return root_id

    def insert_root_id(ticket_id: int, root_id: str):
        update_query = """
            update ticket 
            set root_id = %s
            where ticket_id = %s
        """
        pg_cur.execute(update_query, (root_id, ticket_id))
        pg_conn.commit()

    def transport_id(ticket_id):
        root_id = get_root_id_from_otrs(ticket_id)
        if root_id:
            insert_root_id(ticket_id, root_id)

    logger.info("Start replication transport_root_id")
    for one_ticket in data_with_tickets:
        transport_id(one_ticket["id"])


def time_accounting(tickets, mysql_cur, pg_cur, pg_conn):
    def select_time_accounting(prepared_ticket_id):
        query = f"""
            select time_unit
            from time_accounting 
            where ticket_id = %s
        """
        mysql_cur.execute(query, prepared_ticket_id)
        try:
            prepared_time_unit = int(mysql_cur.fetchone()[0])
        except TypeError:
            prepared_time_unit = None
        return prepared_time_unit

    def insert_time_accounting(prepared_ticket_id, prepared_time_unit):
        update_query = """
            update ticket 
            set ticket_time_unit = %s
            where ticket_id = %s
        """
        pg_cur.execute(update_query, (prepared_time_unit, prepared_ticket_id))
        pg_conn.commit()

    time_unit_numbers = 0
    for ticket in tickets:
        ticket_id = ticket["id"]
        time_unit = select_time_accounting(ticket_id)
        if time_unit:
            insert_time_accounting(ticket_id, time_unit)
            time_unit_numbers = time_unit_numbers + 1
    logger.info("Get %d time unit numbers", time_unit_numbers)


def replication_otrs_to_dwh(execution_date, replication_otrs_to_dwh_settings):
    execution_start_date = parse(execution_date).replace(tzinfo=timezone.utc)
    execution_date = execution_start_date.strftime("%Y-%m-%d %H:%M:%S.%f")

    logging.info("Start time to extract the otrs data: %s", execution_date)

    dwh_connection_id = replication_otrs_to_dwh_settings["dwh_connection_id"]
    dwh_connection_settings = BaseHook.get_connection(dwh_connection_id)
    pg_conn = connect_to_pg(dwh_connection_settings)
    pg_cur = pg_conn.cursor()

    otrs_connection_id = replication_otrs_to_dwh_settings["otrs_connection_id"]
    otrs_connection_settings = BaseHook.get_connection(otrs_connection_id)
    mysql_conn = connect_to_mysql(otrs_connection_settings)
    mysql_cur = mysql_conn.cursor()

    get_data = GetData(mysql_conn, mysql_cur, execution_start_date)

    ticket_state_type(pg_conn, pg_cur, get_data)
    ticket_state(pg_conn, pg_cur, get_data)
    ticket_type(pg_conn, pg_cur, get_data)
    ticket_priority(pg_conn, pg_cur, get_data)
    ticket_lock_type(pg_conn, pg_cur, get_data)
    queue(pg_conn, pg_cur, get_data)
    service(pg_conn, pg_cur, get_data)
    sla(pg_conn, pg_cur, get_data)
    users(pg_conn, pg_cur, get_data)
    customer_company(pg_conn, pg_cur, get_data)
    customer_user(pg_conn, pg_cur, get_data)
    ticket_history_type(pg_conn, pg_cur, get_data)
    data_with_tickets = ticket(pg_conn, pg_cur, get_data)
    transport_root_id(data_with_tickets, mysql_cur, pg_cur, pg_conn)
    time_accounting(data_with_tickets, mysql_cur, pg_cur, pg_conn)
    ticket_history(pg_conn, pg_cur, get_data)

    closure_codes(pg_conn, pg_cur, get_data, data_with_tickets)
    action_performed(pg_conn, pg_cur, get_data)

    pg_conn.close()
    mysql_conn.close()
