# frames_db.py

import db
import users



class connection(db.connection):
    def get_user(self, user_name):
        return users.get_user(self, login)

    def get_user_id(self, user_name):
        return users.get_user_id(self, login)


def sqlite3_db(database_name='frames.db'):
    import sqlite3

    class sqlite3_connection(connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db_conn.row_factory = sqlite3.Row
            self.reset_cursor()
            self.execute('PRAGMA foreign_keys = 1')

    return db.db(sqlite3, connection=sqlite3_connection)
