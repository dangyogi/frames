# sqlite3_sql.py

from frames import sqlite3_conn as get_conn



from sql_generator import (
    asbool, aslist, separate, gen, database as sql_gen_database,
    schema as sql_gen_schema, table as sql_gen_table, column, sql_type,
    table_constraint, index
);



class database(sql_gen_database):
    can_alter_database = False

    def create(self, outfile):
        if hasattr(self.database, 'schema'):
            #print(f"database.create, gen(schema, {len(self.database.schema)}")
            gen(schema, self.database.schema, outfile)


class schema(sql_gen_schema):
    default_name = 'main'
    can_alter_schema = False

    def create_schema_ddl(self, outfile):
        r'''Writes trailing ';\n'
        '''
        pass


class table(sql_gen_table):
    can_alter_table_schema = False
    can_alter_table_name = True
    can_alter_column_name = True
    can_alter_column = False
    can_drop_column = False
    can_add_column = True
    can_alter_index = False

    def create_options(self, outfile):
        r'''Writes initial space, no termination.
        '''
        if not asbool(getattr(self.table, 'with_oid', 'true')):
            outfile.write(" WITHOUT ROWID")

