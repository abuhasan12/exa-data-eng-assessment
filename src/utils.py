import psycopg2

class PostgresClient():
    def __init__(self, host, port, user, password, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def switch_database(self, database):
        self.database = database
    
    def connect_default(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()
        return self.cursor
    
    def connect(self, database=None):
        if not database:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        else:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=database,
                user=self.user,
                password=self.password
            )
        self.cursor = self.conn.cursor()
        return self.cursor

    def add_sql(self, sql):
        self.cursor.execute(sql)

    def add_sql_file(self, file_path):
        with open(file_path, 'r') as f:
            sql = f.read()
        self.add_sql(sql)
    
    def run_query(self, sql):
        self.add_sql(sql)
        return self.cursor.fetchall()
    
    def commit_sql(self):
        self.conn.commit()

    def enable_autocommit(self):
        self.conn.autocommit = True

    def disable_autocommit(self):
        self.conn.autocommit = False

    def close_connection(self):
        self.cursor.close()
        self.conn.close()