# frames_db.py

from itertools import chain

import db
from versions import version_obj


def sqlite3_db():
    import sqlite3

    class Row:
        r'''Makes sqlite3.Row mutable and look more like dicts.

        Disables treating Rows as tuples.

        Implements:
            - get(name, [default])
            - __getitem__  # raises KeyError if not found
            - keys()
            - __contains__
            - __setitem__  # in extra_values, overrides row values
            - __delitem__  # only for extra_values

        __iter__, items and values are not defined.
        '''
        def __init__(self, cursor, row):
            self.row = sqlite3.Row(cursor, row)
            self.extra_values = {}

        def __getitem__(self, name):
            assert isinstance(name, str)
            try:
                return self.extra_values[name]
            except KeyError:
                try:
                    return self.row[name]
                except IndexError:
                    raise KeyError(name)

        def get(self, name, default=None):
            try:
                return self[name]
            except KeyError:
                return default

        def keys(self):
            return chain(self.extra_values.keys(), self.row.keys())

        def __contains__(self, name):
            try:
                # Take advantage of case-insensitivity in sqlite3.Row
                self[name]
            except KeyError:
                return False
            return True

        def __setitem__(self, name, value):
            self.extra_values[name] = value

        def __delitem__(self, name):
            del self.extra_values[name]

        def __iter__(self):
            raise NotImplemented

    class sqlite3_connection(db.connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db_conn.row_factory = Row
            self.reset_cursor()
            self.execute('PRAGMA foreign_keys = 1')

        def at_version(self, user, version_name, for_update=False):
            return version_obj(self, user, version_name, for_update)

    return db.db(sqlite3, connection=sqlite3_connection)
