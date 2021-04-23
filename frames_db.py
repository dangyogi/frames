# frames_db.py

import db
from versions import version_obj


def sqlite3_db():
    import sqlite3

    class sqlite3_connection(db.connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db_conn.row_factory = sqlite3.Row
            self.reset_cursor()
            self.execute('PRAGMA foreign_keys = 1')

        def at_version(self, user, version_name, for_update=False):
            return version_obj(self, user, version_name, for_update)

    return db.db(sqlite3, connection=sqlite3_connection)
