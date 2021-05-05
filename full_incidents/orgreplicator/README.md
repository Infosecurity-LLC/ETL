# org_replicator

Реплицирует данные о организациях из базы `streamers` в базу `dwh`.

Перед запуском DAGа требуется в AirFlow создать Connection,
 где указать следующие параметры, параметр `schema` оставить пустым

```
Conn Id: test_postgres
Conn Type: Postgres
Host: <host>
Login: <login>
Password: <pass>
Port: <port>
