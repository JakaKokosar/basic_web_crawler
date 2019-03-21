import psycopg2

"""
mkdir -p $HOME/docker/volumes/postgres
docker run --rm --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data postgres
docker exec -it pg-docker psql -U postgres
"""


db_auth = {
    'user': 'postgres',
    'password': 'docker',
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'postgres'
}

class DBConn:
    def __init__(self):
        self.connection = psycopg2.connect(**db_auth)
        self.cursor = self.connection.cursor()
        self.is_occupied = False


# Pool of connection objects to minimize number of connections to the db
# since we have a fixed number of workers, the pool can also have a constant number of connections
class DBConnPool:
    def __init__(self, pool_size):
        self.connections = [DBConn() for _ in range(pool_size)]

    def request_connection(self):
        non_occupied_connection = [conn for conn in self.connections if not conn.is_occupied]
        if len(non_occupied_connection) == 0:
            return None
        conn = non_occupied_connection[0]
        conn.is_occupied = True
        return conn

    def release_connection(self, conn):
        conn.is_occupied = False
        conn.connection.commit()


class DBApi:
    def __init__(self, conn):
        self.conn = conn

    # save `site` and return ID
    def insert_site(self, domain, robots_content, sitemap_content):
        sql = "insert into crawldb.site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s) RETURNING ID"
        cursor = self.conn.cursor
        cursor.execute(sql, (domain, robots_content, sitemap_content))
        id = cursor.fetchone()[0]
        return id


# execute this to initialize DB for the first time
# def db_init():
#     with psycopg2.connect(**db_auth) as conn:
#         with conn.cursor() as cursor:
#             with open('crawldb.sql', 'r') as fp:
#                 create_table_query = fp.read()
#                 cursor.execute(create_table_query)
#                 print("Table created successfully in PostgreSQL ")
#
#     print("PostgreSQL connection is closed")


# def db_connect():
#     return psycopg2.connect(**db_auth)


if __name__ == "__main__":
    # create_tables()
    pool = DBConnPool(4)
    conn = pool.request_connection()
    api = DBApi(conn)
    api.insert_site("http://gov.si", "test", "test2")
    pool.release_connection(conn)

