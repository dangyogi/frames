# sqlite3_sql.py

import sqlite3 as db_module


from sql_generator import (
    asbool, aslist, separate, gen, database as sql_gen_database,
    schema as sql_gen_schema, table as sql_gen_table, column, sql_type,
    table_constraint, index
);



class database(sql_gen_database):
    def create(self, outfile):
        if hasattr(self.database, 'schema'):
            gen(schema, self.database.schema, outfile)


class schema(sql_gen_schema):
    default_name = 'main'

    def create_schema_ddl(self, outfile):
        r'''Writes trailing ';\n'
        '''
        pass


class table(sql_gen_table):
    def create_options(self, outfile):
        r'''Writes initial space, no termination.
        '''
        if not asbool(getattr(self.table, 'with_oid', 'true')):
            outfile.write(" WITHOUT ROWID")

