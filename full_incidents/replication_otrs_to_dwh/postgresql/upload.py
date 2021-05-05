from exceptions import OTRSFieldMissingError, PostgreSQLFieldMissingError

import hashlib
import logging

logger = logging.getLogger('replication_otrs_to_dwh')


class UploadData:
    def __init__(self, pg_conn, pg_cur):
        self.pg_conn = pg_conn
        self.pg_cur = pg_cur

    def upload(self, rows):
        for one_row in rows:
            self.insert(one_row)
        self.pg_conn.commit()

    def insert(self, *kwargs):
        # впоследствии переопределяется для каждого класса загрузки в таблицу
        pass

    def is_table_empty(self, table):
        query = f"""
                select COUNT(*) from {table}
                """
        self.pg_cur.execute(query)
        row_number = self.pg_cur.fetchone()[0]
        is_table_empty_value = False
        if row_number == 0:
            is_table_empty_value = True
        return is_table_empty_value


class UploadIntoTicketStateType(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
        insert into ticket_state_type (id, name)
        VALUES (%s , %s)
        ON CONFLICT (id) do update set name = excluded.name
        """
        self.pg_cur.execute(query, (row_id, row_name))


class UploadIntoTicketState(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        row_type = one_row.get("type_id")
        query = """
            insert into ticket_state (id, name, state_type) 
            values (%s, %s, %s)
            on conflict (id) do update set name = excluded.name, state_type = excluded.state_type
        """
        self.pg_cur.execute(query, (row_id, row_name, row_type))


class UploadIntoTicketType(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
        insert into ticket_type (id, name)
        VALUES (%s , %s)
        ON CONFLICT (id) do update set name = excluded.name
        """
        self.pg_cur.execute(query, (row_id, row_name))


class UploadIntoTicketPriority(UploadData):
    def insert(self, one_row):
        row_otrs_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
                insert into ticket_priority (name, otrs_id)
                VALUES (%s , %s)
                ON CONFLICT (otrs_id) do update set name = excluded.name
                """
        self.pg_cur.execute(query, (row_name, row_otrs_id))


class UploadIntoTicketLockType(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
                insert into ticket_lock_type (id, name)
                VALUES (%s , %s)
                ON CONFLICT (id) do update set name = excluded.name
                """
        self.pg_cur.execute(query, (row_id, row_name))


class UploadIntoQueue(UploadData):
    def insert(self, one_row):
        row_otrs_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
                        insert into queue (name, otrs_id)
                        VALUES (%s , %s)
                        ON CONFLICT (otrs_id) do update set name = excluded.name
                        """
        self.pg_cur.execute(query, (row_name, row_otrs_id))


class UploadIntoService(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        row_crit = one_row.get("criticality")
        query = """
            insert into service (id, name, criticality) 
            values (%s, %s, %s)
            on conflict (id) do update set name = excluded.name, criticality = excluded.criticality
        """
        self.pg_cur.execute(query, (row_id, row_name, row_crit))


class UploadIntoSla(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        row_first_response_time = one_row.get("first_response_time")
        row_first_response_notify = one_row.get("first_response_notify")
        row_update_time = one_row.get("update_time")
        row_update_notify = one_row.get("update_notify")
        row_solution_time = one_row.get("solution_time")
        row_solution_notify = one_row.get("solution_notify")
        query = """
            insert into sla (id, name, first_response_time,
                 first_response_notify, update_time,
                 update_notify, solution_time, solution_notify)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (id) do update set name = excluded.name, first_response_time = excluded.first_response_time,
            first_response_notify = excluded.first_response_notify, update_time = excluded.update_time,
            update_notify = excluded.update_notify, solution_time = excluded.solution_time,
            solution_notify = excluded.solution_notify
        """
        self.pg_cur.execute(query, (row_id, row_name, row_first_response_time, row_first_response_notify,
                                    row_update_time, row_update_notify, row_solution_time, row_solution_notify))


class UploadIntoUsers(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_login = one_row.get("login")
        row_first_name = one_row.get("first_name")
        row_last_name = one_row.get("last_name")
        query = """
                    insert into users (login, otrs_id, first_name, last_name) 
                    values (%s,%s, %s, %s)
                    on conflict (otrs_id) do update set login = excluded.login, first_name = excluded.first_name,
                    last_name = excluded.last_name
                """
        self.pg_cur.execute(query, (row_login, row_id, row_first_name, row_last_name))


class UploadIntoOrganizations(UploadData):
    def insert(self, one_row):
        otrs_id = one_row.get("customer_id")
        row_name = one_row.get("name")
        query = """
                    update organizations
                    set org_otrs_id = %s
                    where name = %s
                """
        self.pg_cur.execute(query, (otrs_id, row_name))


class UploadIntoCustomerUser(UploadData):
    def insert(self, one_row):
        login = one_row.get("login")
        email = one_row.get("email")
        first_name = one_row.get("first_name")
        last_name = one_row.get("last_name")
        phone = one_row.get("phone")
        mobile = one_row.get("mobile")
        cust_otrs_id = str(one_row.get("id"))
        cust_id = one_row.get("customer_id")
        query = """
            insert into customer_user (login, email, customer_id, first_name, last_name,
                                        phone, mobile, customer_otrs_id)
            values (%s, %s, (select id from organizations where org_otrs_id = %s), %s, %s, %s, %s, %s)
            on conflict (customer_otrs_id) do update set login = excluded.login,
            email = excluded.email, customer_id = excluded.customer_id, first_name = excluded.first_name,
            last_name = excluded.last_name, phone = excluded.phone, mobile = excluded.mobile
        """
        self.pg_cur.execute(query, (login, email, cust_id, first_name, last_name, phone, mobile, cust_otrs_id))


class UploadIntoTicketHistoryType(UploadData):
    def insert(self, one_row):
        row_id = one_row.get("id")
        row_name = one_row.get("name")
        query = """
            insert into ticket_history_type (id, name)
            VALUES (%s , %s)
            ON CONFLICT (id) do update set name = excluded.name
        """
        self.pg_cur.execute(query, (row_id, row_name))


class UploadIntoTicket(UploadData):
    def select_required_fields(self, ticket_id, ticket_priority_id, queue_id):
        query_ticket_priority_id = f"""
                                    select id from ticket_priority where otrs_id = {ticket_priority_id}
                                    """
        self.pg_cur.execute(query_ticket_priority_id)
        ticket_priority_id = self.pg_cur.fetchone()[0]
        if not ticket_priority_id:
            raise PostgreSQLFieldMissingError("Ticket with id %s NOT uploaded, because field ticket_priority_id is "
                                              "missing in PostgreSQL", ticket_id)
        query_queue_id = f"""
                            select id from queue where otrs_id = {queue_id}
                            """
        self.pg_cur.execute(query_queue_id)
        queue_id = self.pg_cur.fetchone()[0]
        if not queue_id:
            raise PostgreSQLFieldMissingError("Ticket with id %s NOT uploaded, because field queue_id is missing "
                                              "in PostgreSQL", ticket_id)
        return ticket_priority_id, queue_id

    def validate_fields(self, ticket_id, ticket_state_id, type_id):
        if not ticket_id:
            raise OTRSFieldMissingError("Ticket NOT uploaded, because field ticket_id is missing in OTRS")
        if not ticket_state_id:
            raise OTRSFieldMissingError("Ticket with id %s NOT uploaded, because field ticket_state_id is missing "
                                        "in OTRS", ticket_id)
        if not type_id:
            raise OTRSFieldMissingError("Ticket with id %s NOT uploaded, because field type_id is missing in OTRS",
                                        ticket_id)

    def insert(self, one_row):
        title = one_row.get("title")
        queue_id = one_row.get("queue_id")
        ticket_lock_id = one_row.get("ticket_lock_id")
        type_id = one_row.get("type_id")
        service_id = one_row.get("service_id")
        sla_id = one_row.get("sla_id")
        ticket_priority_id = one_row.get("ticket_priority_id")
        ticket_state_id = one_row.get("ticket_state_id")
        customer_id = one_row.get("customer_id")
        timeout = one_row.get("timeout")
        until_time = one_row.get("until_time")
        escalation_time = one_row.get("escalation_time")
        escalation_update_time = one_row.get("escalation_update_time")
        escalation_response_time = one_row.get("escalation_response_time")
        escalation_solution_time = one_row.get("escalation_solution_time")
        row_id = one_row.get("id")
        tn = one_row.get("tn")
        create_time = one_row.get("create_time")
        change_time = one_row.get("change_time")
        customer_user_id = one_row.get("customer_user_id")

        self.validate_fields(ticket_id=row_id,
                             ticket_state_id=ticket_state_id,
                             type_id=type_id)

        selected_ticket_priority_id, selected_queue_id = self.select_required_fields(
            ticket_id=row_id,
            ticket_priority_id=ticket_priority_id,
            queue_id=queue_id)
        query = """
            insert into ticket (title, queue_id, ticket_lock_id, type_id, service_id,
                        sla_id, ticket_priority_id, ticket_state_id, customer_id,
                        customer_user_id, timeout, until_time, escalation_time,
                        escalation_update_time, escalation_response_time,
                        escalation_solution_time, ticket_id,
                        ticket_tn, create_time, change_time)
            values (
            %s,
            %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             (select id from organizations where org_otrs_id = %s and org_otrs_id IS NOT NULL and org_otrs_id != ''),
             (select id from customer_user where login = %s),
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s
            )
            on conflict (ticket_id) do update set title = excluded.title, queue_id = excluded.queue_id,
            ticket_lock_id = excluded.ticket_lock_id, type_id = excluded.type_id, service_id = excluded.service_id,
            sla_id = excluded.sla_id, ticket_priority_id = excluded.ticket_priority_id,
            ticket_state_id = excluded.ticket_state_id, customer_id = excluded.customer_id,
            customer_user_id = excluded.customer_user_id, timeout = excluded.timeout, until_time = excluded.until_time,
            escalation_time = excluded.escalation_time, escalation_update_time = excluded.escalation_update_time,
            escalation_response_time = excluded.escalation_response_time, 
            escalation_solution_time = excluded.escalation_solution_time, ticket_tn = excluded.ticket_tn,
            create_time = excluded.create_time, change_time = excluded.change_time
        """
        self.pg_cur.execute(query, (title, selected_queue_id, ticket_lock_id, type_id, service_id, sla_id,
                                    selected_ticket_priority_id, ticket_state_id, customer_id, customer_user_id,
                                    timeout, until_time, escalation_time, escalation_update_time,
                                    escalation_response_time,escalation_solution_time, row_id, tn, create_time,
                                    change_time))


class UploadIntoTicketHistory(UploadData):
    def select_queue_ids(self):
        query = f"""
                select id, otrs_id
                from queue
                """
        self.pg_cur.execute(query)
        rows = self.pg_cur.fetchall()

        queue_dict = {}
        for row in rows:
            queue_dict[row[1]] = row[0]
        return queue_dict

    def select_users_ids(self):
        query = f"""
                select id, otrs_id
                from users
                """
        self.pg_cur.execute(query)
        rows = self.pg_cur.fetchall()

        users_dict = {}
        for row in rows:
            users_dict[row[1]] = row[0]
        return users_dict

    def select_ticket_history_identity_hash(self, ticket_history_string):
        identity_hash = hashlib.md5(ticket_history_string.encode('utf-8')).hexdigest()
        query_hash = f"""
                      select id from ticket_history where ticket_history_identity_hash = '{identity_hash}'
                      """
        self.pg_cur.execute(query_hash)
        identity_hash_from_pg = self.pg_cur.fetchone()
        return identity_hash_from_pg, identity_hash

    def upload(self, rows):
        """
        Переопределённый метод класса UploadData. Из DWH подгружаются очереди и пользователи. Затем они сопоставляются
        в цикле cо строкам из OTRS. Сформированные строки передаются в функцию insert.
        """

        inserted_ticket_history = 0
        existing_history_ticket = 0

        queue_dict = self.select_queue_ids()
        user_dict = self.select_users_ids()
        for one_row in rows:
            queue_id = queue_dict.get(one_row["queue_id"])
            if not queue_id:
                raise PostgreSQLFieldMissingError("Ticket history NOT uploaded, because queue_id %s is missing "
                                                  "in PostgreSQL", one_row["queue_id"])
            one_row["queue_id"] = queue_id
            user_id = user_dict.get(one_row["owner_id"])
            if not user_id:
                raise PostgreSQLFieldMissingError("Ticket history NOT uploaded, because owner_id %s is missing "
                                                  "in PostgreSQL", one_row["owner_id"])
            one_row["owner_id"] = user_id
            inserted_ticket_history, existing_history_ticket = \
                self.insert(one_row, inserted_ticket_history, existing_history_ticket)
        self.pg_conn.commit()
        logging.info("Number of inserted ticket histories %d, number of existing ticket histories %d",
                     inserted_ticket_history, existing_history_ticket)

    def insert(self, one_row, inserted_ticket_history, existing_history_ticket):
        """
        Загрузка ticket_history из OTRS в DWH.
        С помощью identity_hash проверяется, существует ли полученная строка в DWH. Если существует, увеличивается
        значение existing_history_ticket. Если нет - загружается в DWH и увеличивается значение inserted_ticket_history.
        """
        ticket_id = one_row.get("ticket_id")
        name = one_row.get("name")
        history_type_id = one_row.get("history_type_id")
        queue_id = one_row.get("queue_id")
        user_id = one_row.get("owner_id")
        create_time = one_row.get("create_time")
        change_time = one_row.get("change_time")

        ticket_history_string = f"{ticket_id} {name} {history_type_id} {queue_id} {user_id} {create_time} " \
                                f"{change_time}"
        identity_hash_from_pg, identity_hash = self.select_ticket_history_identity_hash(ticket_history_string)

        if not identity_hash_from_pg:
            query = """
                insert into ticket_history (ticket_id, name, history_type_id,
                    queue_id, create_time, change_time, user_id, ticket_history_identity_hash)
                values (
                 (select id from ticket where ticket.ticket_id = %s),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s
                )
            """
            self.pg_cur.execute(query, (ticket_id, name, history_type_id, queue_id, create_time, change_time, user_id,
                                        identity_hash))
            inserted_ticket_history += 1
        else:
            existing_history_ticket += 1
        return inserted_ticket_history, existing_history_ticket


class UploadClosureCode(UploadData):
    def insert(self, one_row):
        query = """
                    insert into closure_code (name, valid_id, comments, create_time, create_by, change_time, 
                    change_by, type_id, otrs_id) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (otrs_id) do update set comments = excluded.comments, 
                    change_time = excluded.change_time,
                    change_by = excluded.change_by
                """
        self.pg_cur.execute(query, (one_row.get("name"),
                                    one_row.get("valid_id"),
                                    one_row.get("comments"),
                                    one_row.get("create_time"),
                                    one_row.get("create_by"),
                                    one_row.get("change_time"),
                                    one_row.get("change_by"),
                                    one_row.get("type_id"),
                                    one_row.get("id"),
                                    ))


class UploadTicketCode(UploadData):
    def insert(self, one_row):
        ticket_id = one_row.get("ticket_id")
        code_id = one_row.get("code_id")

        query_ticket_id = f"""
                            select id from ticket where ticket_id={ticket_id}
                           """
        self.pg_cur.execute(query_ticket_id)
        ticket_id_from_pg = self.pg_cur.fetchone()[0]

        query = f"""
                    select id from ticket_code where ticket_id={ticket_id_from_pg}
                """
        self.pg_cur.execute(query)
        ticket_code_id = self.pg_cur.fetchone()

        query = f"""
                    insert into ticket_code(ticket_id, code_id) 
                    values ({ticket_id_from_pg}, {code_id})
                """
        if ticket_code_id:
            query = f"""
                        update ticket_code 
                        set code_id = {code_id}
                        where ticket_id = {ticket_id_from_pg}
                    """
        self.pg_cur.execute(query)


class UploadArticle(UploadData):
    def insert(self, one_row):
        query = """
                    insert into article (ticket_id, article_sender_type_id, communication_channel_id, 
                    is_visible_for_customer, search_index_needs_rebuild, insert_fingerprint, create_time,
                    create_by, change_time, change_by, otrs_id) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (otrs_id) do nothing 
                """

        self.pg_cur.execute(query, (one_row.get("ticket_id"),
                                    one_row.get("article_sender_type_id"),
                                    one_row.get("communication_channel_id"),
                                    one_row.get("is_visible_for_customer"),
                                    one_row.get("search_index_needs_rebuild"),
                                    one_row.get("insert_fingerprint"),
                                    one_row.get("create_time"),
                                    one_row.get("create_by"),
                                    one_row.get("change_time"),
                                    one_row.get("change_by"),
                                    one_row.get("id"),
                                    ))


class UploadArticleDataMime(UploadData):
    def insert(self, one_row):
        query = """
                    insert into article_data_mime (article_id, a_from, a_reply_to, a_to, a_cc, a_bcc, a_subject, a_message_id,
             a_message_id_md5, a_in_reply_to, a_references, a_content_type, a_body, incoming_time,
             content_path, create_time, create_by, change_time, change_by)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (article_id) do nothing 
                """
        self.pg_cur.execute(query, (one_row.get("article_id"),
                                    one_row.get("a_from"),
                                    one_row.get("a_reply_to"),
                                    one_row.get("a_to"),
                                    one_row.get("a_cc"),
                                    one_row.get("a_bcc"),
                                    one_row.get("a_subject"),
                                    one_row.get("a_message_id"),
                                    one_row.get("a_message_id_md5"),
                                    one_row.get("a_in_reply_to"),
                                    one_row.get("a_references"),
                                    one_row.get("a_content_type"),
                                    one_row.get("a_body"),
                                    one_row.get("incoming_time"),
                                    one_row.get("content_path"),
                                    one_row.get("create_time"),
                                    one_row.get("create_by"),
                                    one_row.get("change_time"),
                                    one_row.get("change_by"),
                                    ))
