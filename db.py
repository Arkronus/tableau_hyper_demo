import psycopg2
import config

class DBWrapper():
    def __init__(self):
        super().__init__()
        conn = psycopg2.connect(dbname=config.dbname, user=config.user, 
                        password=config.password, host=config.host)
        self.cursor = conn.cursor()

    def get_new_data(self, date):
        self.cursor.execute("""
        select * from superstore where order_date >= %(date)s
        """, 
        {'date': date})

        return self.cursor.fetchall()
