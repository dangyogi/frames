# sql_generator.py

import frames


def asbool(x):
    if x.lower() == 'true':
        return True
    if x.lower() == 'false':
        return False
    return bool(int(x))


def aslist(x):
    if isinstance(x, (frames.slot_list, list, tuple)):
        return x
    return [x]


def seperate(lines, seperator='\n', first_sep=''):
    for i, line in enumerate(lines, 1):
        if i == 1:
            yield first_sep, line
        else:
            yield seperator, line


class database:
    def __init__(self, frame):
        self.database = frame

    def create(self, outfile):
        self.create_database(outfile)
        if hasattr(self.database, 'schema'):
            outfile.write('\n')
            schema(self.database.schema).create(outfile)
        if hasattr(self.database, 'table'):
            outfile.write('\n')
            table(self.database.table).create(outfile)

    def create_database(self, outfile):
        r'''Writes through ';\n'.
        '''
        outfile.write(f"CREATE DATABASE {self.name}{self.create_options()};\n")

    def create_options(self):
        r'''Returns WITH options string.

        Returned string starts with a space and has no terminator at the end
        of it.
        '''
        ans = []
        for attr in ("owner", "template", "encoding", "lc_collate", "lc_ctype",
                     "tablespace", "allow_connections", "connection_limit",
                     "is_template"):
            ans.append(f"{attr.upper()} = {getattr(self.frame, attr)}")
        if ans:
            return " WITH\n" + '\n    '.join(ans)


class schema:
    def __init__(self, schemas):
        self.schemas = schemas

    def create(self, outfile):
        for sep, s in seperate(aslist(self.schemas)):
            outfile.write(sep)
            self.create_schema(s, outfile)

    def create_schema(self, schema, outfile):
        r'''Writes through ';\n'
        '''
        outfile.write(
          f"CREATE SCHEMA {schema.name}{self.create_auth(schema)};\n")

    def create_auth(self, schema):
        r'''Returns auth string.

        Auth string starts with space and has no termination.
        '''
        if hasattr(schema, 'authorization'):
            return f" AUTHORIZATION {schema.authorization}"


class table:
    def __init__(self, tables):
        self.tables = tables

    def create(self, outfile):
        for sep, t in seperate(aslist(self.tables)):
            outfile.write(sep)
            self.create_table(t, outfile)

    def create_table(self, table, outfile):
        outfile.write(
          f"CREATE TABLE{self.pre_name(table)} {table.table_name}"
	  f"{self.post_name(table)} (\n")
        if hasattr(table, 'column') or hasattr(table, 'constraint'):
            if hasattr(table, 'column'):
                # no termination on final line:
                column(table, table.column).create(outfile)

                if hasattr(table, 'constraint'):
                    outfile.write(',\n')

                    # no termination on final line:
                    table_constraint(table, table.constraint).create(outfile)
            else:
                # no termination on final line:
                table_constraint(table, table.constraint).create(outfile)
        outfile.write('\n)')
        self.create_options(table, outfile)
        outfile.write(';\n')
        if hasattr(table, 'index'):
            outfile.write('\n')
            index(table, table.index).create(outfile)

    def pre_name(self, table):
        r'''Returns string with initial space, no termination.
        '''
        return ''

    def post_name(self, table):
        r'''Returns string with initial space, no termination.
        '''
        return ''

    def create_options(self, table, outfile):
        r'''Writes initial space, no termination.
        '''
        pass


class column:
    def __init__(self, table, columns):
        self.table = table
        self.columns = columns

    def create(self, outfile):
        for sep, c in seperate(aslist(self.columns), ',\n'):
            outfile.write(sep)
            self.create_column(c, outfile)

    def create_column(self, column, outfile):
        r'''Writes lines indented 4 spaces with no termination.
        '''
        outfile.write(
          f"    {column.name} {sql_type(self.table, column).create()}")
        if hasattr(column, 'collate'):
            outfile.write(f" COLLATE {column.collate}")
        if asbool(column.primary_key):
            outfile.write(" PRIMARY KEY")
        if not asbool(column.nullable):
            outfile.write(" NOT NULL")
        if hasattr(column, 'links_to') and asbool(column.check_foreign_key):
            outfile.write(f" REFERENCES {column.links_to}")


class sql_type:
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def create(self):
        r'''Returns a string with no leading or trailing spaces.
        '''
        return getattr(self, self.column.type.lower())()

    def integer(self):
        if hasattr(self.column, 'bit_size'):
            if int(self.column.bit_size) == 16:
                return "SMALLINT"
            if int(self.column.bit_size) == 32:
                return "INTEGER"
            elif int(self.column.bit_size) == 64:
                return "BIGINT"
            else:
                raise AssertionError(
                        f"{self.table.table_name}.{self.column.name} has "
                        "unknown 'bit_size' value: {self.column.bit_size}")
        else:
            return "INTEGER"

    def float(self):
        if hasattr(self.column, 'bit_size'):
            if int(self.column.bit_size) == 32:
                return "REAL"
            elif int(self.column.bit_size) == 64:
                return "DOUBLE PRECISION"
            else:
                raise AssertionError(
                        f"{self.table.table_name}.{self.column.name} has "
                        "unknown 'bit_size' value: {self.column.bit_size}")
        else:
            return "DOUBLE PRECISION"

    def decimal(self):
        if hasattr(self.column, 'num_digits'):
            if hasattr(self.column, 'num_decimals'):
                return f"DECIMAL({self.column.num_digits}, " \
                       f"{self.column.num_decimals})"
            else:
                return f"DECIMAL({self.column.num_digits})"
        elif hasattr(self.column, 'num_decimals'):
            raise AssertionError(
                    f"{self.table.table_name}.{self.column.name} has "
                    "'num_decimals', so must also have 'num_digits'")
        return "DECIMAL"
    
    def boolean(self):
        return "BOOLEAN"

    def string(self):
        if hasattr(self.column, 'max_len'):
            return f"VARCHAR({self.column.max_len})"
        return "VARCHAR"

    def timestamp(self):
        if hasattr(self.column, 'fractional_second_digits'):
            ans = f"TIMESTAMP({self.column.max_len})"
        else:
            ans = "TIMESTAMP"
        if hasattr(self.column, 'with_time_zone') and \
           asbool(self.column.with_time_zone):
            return ans + ' WITH TIME ZONE'
        return ans


class table_constraint:
    def __init__(self, table, constraints):
        self.table = table
        self.constraints = constraints

    def create(self, outfile):
        for sep, c in seperate(aslist(self.constraints), ',\n'):
            outfile.write(sep)
            self.create_constraint(c, outfile)
    
    def create_constraint(self, constraint, outfile):
        getattr(self, constraint.type.lower())(constraint, outfile)

    def primary_key(self, constraint, outfile):
        outfile.write(
          f"    PRIMARY KEY ({', '.join(aslist(constraint.column))})")


class index:
    def __init__(self, table, index):
        self.table = table
        self.index = index

    def create(self, outfile):
        for i in aslist(self.index):
            self.create_index(i, outfile)
    
    def create_index(self, index, outfile):
        outfile.write(f"CREATE{self.unique(index)} INDEX ")
        if hasattr(self.table, 'schema'):
            outfile.write(self.table.schema + '.')
        outfile.write(
          f"{self.table.table_name}__{'__'.join(aslist(index.columns))}__idx")
        outfile.write(
          f" ON {self.table.table_name}({', '.join(aslist(index.columns))})")
        if hasattr(index, 'where'):
            outfile.write(f" WHERE {index.where}")
        outfile.write(';\n')

    def unique(self, index):
        if asbool(getattr(index, 'unique', 'false')):
            return " UNIQUE"
        return ''


def create(sql_gen, frame, outfile):
    getattr(sql_gen, frame.isa.lower())(frame).create(outfile)



if __name__ == '__main__':
    import sys
    import argparse
    import sqlite3_sql as sql_gen_module
    import frames

    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parser.add_argument('frame_label')
    parser.add_argument('versions', nargs='+')

    args = parser.parse_args()

    if args.command == 'create':
        def add_row_factory(conn):
            conn.row_factory = sql_gen_module.db_module.Row
        db_obj = frames.db(sql_gen_module.db_module, "test.db",
                           post_connect=add_row_factory)
        db_obj.execute("""SELECT user_id FROM User WHERE name = 'bruce';""")
        user_id, = db_obj.fetchone()
        version_obj = db_obj.at_versions(user_id, *args.versions)
        create(sql_gen_module, version_obj.get_frame(args.frame_label),
               sys.stdout)
    else:
        raise AssertionError(f"Unknown command: {args.command}")
