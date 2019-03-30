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
    def __init__(self, conn: DBConn):
        self.conn = conn

    # inserts / updates ...

    # save `site` and return ID
    def insert_site(self, domain, robots_content, sitemap_content):
        sql = "INSERT INTO crawldb.site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s) RETURNING ID"
        return self._execute_one(sql, (domain, robots_content, sitemap_content))

    # save `page` linked to specific `site` and return ID
    def insert_page(self, site_id, page_type_code, url, html_content, http_status_code, accessed_time):
        try:
            sql = "INSERT INTO crawldb.page (site_id, page_type_code, url, html_content, http_status_code, accessed_time) VALUES (%s, %s, %s, %s, %s, %s) RETURNING ID"
            return self._execute_one(sql, (site_id, page_type_code, url, html_content, http_status_code, accessed_time))
        except Exception as e:
            self.conn.connection.rollback()
            # TODO: - DUPLICATES SHOULDN'T COME THROUGH, BUT THEY DO :S
            return None

    # update existing page
    def update_page(self, page_id, page_type_code, html_content, http_status_code, accessed_time):
        sql = "UPDATE crawldb.page set page_type_code = %s, html_content = %s, http_status_code = %s, accessed_time = %s WHERE id = %s;"
        self.conn.cursor.execute(sql, (page_type_code, html_content, http_status_code, accessed_time, page_id))

    # save `page_data` linked to specific `page` and return ID
    def insert_page_data(self, page_id, data_type_code, data):
        sql = "INSERT INTO crawldb.page_data (page_id, data_type_code, data) VALUES (%s, %s, %s) RETURNING ID"
        return self._execute_one(sql, (page_id, data_type_code, data))

    # save `image`
    def insert_image(self, page_id, filename, content_type, data, accessed_time):
        sql = "INSERT INTO crawldb.image (page_id, filename, content_type, data, accessed_time) VALUES (%s, %s, %s, %s, %s) RETURNING ID"
        return self._execute_one(sql, (page_id, filename, content_type, data, accessed_time))

    def remove_page(self, url, time):
        page_id = self.page_for_url(url)
        self.update_page(page_id, "UNKNOWN", None, 500, time)

    # save `link`
    def insert_link(self, page_id_from, page_id_to):
        sql = "INSERT INTO crawldb.link (from_page, to_page) VALUES (%s, %s)"
        return self.conn.cursor.execute(sql, (page_id_from, page_id_to))

    # save `data`
    def insert_page_data(self, page_id, data_type_code, data):
        sql = "INSERT INTO crawldb.page_data (page_id, data_type_code, data) VALUES (%s, %s, %s)"
        return self._execute_one(sql, (page_id, data_type_code, data))

    # selections ...

    # select all from `page`
    def select_all_pages(self):
        sql = "SELECT * FROM crawldb.page"
        return self._execute_all(sql, ())

    # find `site` with `domain`
    def site_id_for_domain(self, domain):
        sql = "SELECT * FROM crawldb.site WHERE domain = %s"
        return self._execute_one(sql, (domain,))

    # find `page` with `site_id` with a given `url`
    def page_id_for_page_in_frontier(self, site_id, url):
        sql = "SELECT * FROM crawldb.page WHERE site_id = %s AND url = %s AND page_type_code = 'FRONTIER'"
        return self._execute_one(sql, (site_id, url))

    # find `page` with `url`
    def page_for_url(self, url):
        sql = "SELECT * FROM crawldb.page WHERE url = %s"
        return self._execute_one(sql, (url,))

    # # find source `page` from given destination `page`
    # def source_page_for_destination_page(self, destination_page_id):
    #     sql = "SELECT * FROM crawldb.link WHERE to_page = %s"
    #     return self._execute(sql, (destination_page_id, ))

    # internal

    def _execute_one(self, sql, data):
        cursor = self.conn.cursor
        cursor.execute(sql, data)
        self.conn.commit()
        result = cursor.fetchone()
        return result[0] if result else None

    def _execute_all(self, sql, data):
        cursor = self.conn.cursor
        cursor.execute(sql, data)
        self.conn.commit()
        result = cursor.fetchall()
        return result

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
