from airflow.hooks.postgres_hook import PostgresHook


def create_connection(postgres_conn_id):
    """Создание подключения к базе к указанной схеме"""
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


def get_organizations(get_org_connection_id ):
    """Получение данных о актуальных организациях"""
    conn, cursor = create_connection(get_org_connection_id)
    query = f"""
        select short_name, name, active_from, active_to from organizations
        where active_to > now() or active_to is Null
    """
    cursor.execute(query)
    data_with_organizations = cursor.fetchall()
    conn.close()
    return data_with_organizations


def org_to_dict(org_data) -> dict:
    """Преобразование списка с данными о организации в словарь с названием всех данных"""
    fields = ['short_name', 'name', 'active_from', 'active_to']
    return dict(zip(fields, org_data))


def push_organizations(organizations, push_org_connection_id):
    """Добавление новой организации, в случае, если такая организация уже есть
     для нее обновляется дата окончания сотрудничества"""
    conn, cursor = create_connection(push_org_connection_id)
    insert_org = """
        insert into organizations
        (short_name, name, active_from, active_to)
        values (%s, %s, timezone('UTC', %s), timezone('UTC', %s))
        on conflict (short_name) do update set active_to = excluded.active_to
    """
    for organization in organizations:
        org_data = org_to_dict(organization)
        short_name = org_data['short_name']
        name = org_data['name']
        active_from = org_data['active_from']
        active_to = org_data['active_to']
        cursor.execute(insert_org, (short_name, name, active_from, active_to))
    conn.commit()
    conn.close()


def orgreplicator(orgreplicator_settings):
    get_org_connection_id = orgreplicator_settings["get_org_connection_id"]
    push_org_connection_id = orgreplicator_settings["push_org_connection_id"]
    organizations = get_organizations(get_org_connection_id)
    push_organizations(organizations, push_org_connection_id)
