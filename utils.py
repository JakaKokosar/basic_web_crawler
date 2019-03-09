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


def db_init():
    with psycopg2.connect(**db_auth) as conn:
        with conn.cursor() as cursor:
            with open('crawldb.sql', 'r') as fp:
                create_table_query = fp.read()
                cursor.execute(create_table_query)
                print("Table created successfully in PostgreSQL ")

    print("PostgreSQL connection is closed")


def db_connect():
    return psycopg2.connect(**db_auth)


if __name__ == "__main__":
    # create_tables()
    conn = db_connect()
    print('Connected to:', conn)
    conn.close()

