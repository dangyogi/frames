# sqlite3_sql.py

import sqlite3 as db_module


from sql_generator import (
    asbool, aslist, seperate, database, schema, table as sql_gen_table,
    column, sql_type, table_constraint, index
);


class table(sql_gen_table):
    def create_options(self, table, outfile):
        r'''Writes initial space, no termination.
        '''
        if not asbool(getattr(table, 'with_oid', 'true')):
            outfile.write(" WITHOUT ROWID")

