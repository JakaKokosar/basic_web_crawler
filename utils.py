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
    def __init__(self, db_auth=db_auth):
        self.connection = psycopg2.connect(**db_auth)
        self.cursor = self.connection.cursor()

    def release(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()


class DBApi:
    def __init__(self, conn):
        self.conn = conn

    # save `site` and return ID
    def insert_site(self, domain, robots_content, sitemap_content):
        sql = "insert into crawldb.site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s) RETURNING ID"
        return self._execute(sql, (domain, robots_content, sitemap_content))

    # save `page` linked to specific `site` and return ID
    def insert_page(self, site_id, page_type_code, url, html_content, http_status_code, accessed_time):
        sql = "insert into crawldb.page (site_id, page_type_code, url, html_content, http_status_code, accessed_time) VALUES (%s, %s, %s, %s, %s, %s) RETURNING ID"
        return self._execute(sql, (site_id, page_type_code, url, html_content, http_status_code, accessed_time))

    # save `page_data` linked to specific `page` and return ID
    def insert_page_data(self, page_id, data_type_code, data):
        sql = "insert into crawldb.page_data (page_id, data_type_code, data) VALUES (%s, %s, %s) RETURNING ID"
        return self._execute(sql, (page_id, data_type_code, data))

    def insert_image(self, page_id, filename, content_type, data, accessed_time):
        sql = "insert into crawldb.image (page_id, filename, content_type, data, accessed_time) VALUES (%s, %s, %s, %s, %s) RETURNING ID"
        return self._execute(sql, (page_id, filename, content_type, data, accessed_time))

    # internal

    def _execute(self, sql, data):
        cursor = self.conn.cursor
        cursor.execute(sql, data)
        self.conn.commit()
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
    conn = DBConn()
    api = DBApi(conn)
    id = api.insert_site("http://gov.si", "test", "test2")
    api.insert_page(id, "HTML", "http:subpage/gov.si", "<html><body></body></html>", 200, "2019-03-21 12:16:11.988000")
    conn.release()
