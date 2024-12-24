from pyarrow import flight
from datafusion import SessionContext

ctx = SessionContext()

test_data = [
    {
        "id": 1,
        "created_at": "2024-09-07 10:01:05",
        "content": "First entry in first minute",
    },
    {
        "id": 2,
        "created_at": "2024-09-07 10:01:45",
        "content": "Second entry in first minute",
    },
    {
        "id": 3,
        "created_at": "2024-09-07 10:03:10",
        "content": "First entry in third minute",
    },
    {
        "id": 4,
        "created_at": "2024-09-07 10:03:55",
        "content": "Second entry in third minute",
    },
]

ctx.from_pylist(test_data, "count_me")

sql = """ SELECT
DATE_BIN(INTERVAL '1 minute', created_at) AS time_window,
        COUNT(*) AS count
    FROM
        count_me

GROUP BY
    time_window
ORDER BY
    time_window;"""

df = ctx.sql(sql)
df.to_arrow_table()
