from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

MYSQL_SETTINGS = {
    "host": "localhost",
    "user": "root",
    "password": "zhangtao552",
}

if __name__ == '__main__':
    reader = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=9,
        blocking=True,
        # 只监听 数据库 chenxi
        only_schemas="chenxi",
        only_events=(WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
    )

    for binlog_stream in reader:
        print()
        for row in binlog_stream.rows:
            print(row)
            print(row.get("data"))
            print("=========")

    reader.close()
