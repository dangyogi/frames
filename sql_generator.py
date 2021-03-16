# sql_generator.py

from functools import partial

import frames


def asbool(x):
    if x.lower() == 'true':
        return True
    if x.lower() == 'false':
        return False
    return bool(int(x))


def aslist(x):
    if isinstance(x, (frames.slot_list, frames.dynamic_slot_list, list, tuple)):
        return x
    return [x]


def separate(lines, separator='\n', first_sep=''):
    for i, line in enumerate(lines, 1):
        if i == 1:
            yield first_sep, line
        else:
            yield separator, line


def gen(class_, l, outfile, gen_fn='create', separator='\n'):
    #print(f"gen({class_}, {l})")
    for sep, x in separate(aslist(l), separator=separator):
        #print(f"gen got sep {sep!r}, x {x!r})")
        outfile.write(sep)
        getattr(class_(x), gen_fn)(outfile)


class database:
    def __init__(self, frame):
        self.database = frame

    def create(self, outfile):
        self.create_database(outfile)
        if hasattr(self.database, 'schema'):
            outfile.write('\n')
            gen(schema, self.database.schema, outfile)

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
    def __init__(self, schema):
        self.schema = schema

    def sql_prefix(self):
        if self.schema.name.lower() not in ('default',
                                            self.default_name.lower()):
            return f"{self.schema.name}."
        return ''

    def create(self, outfile):
        r'''Writes through ';\n'
        '''
        self.create_schema_ddl(outfile)
        #print("schema.create", self.schema.name)
        if hasattr(self.schema, 'table'):
            #print(f"schema.create, gen(table, {len(self.schema.table)}")
            gen(partial(table, self), self.schema.table, outfile)

    def create_schema_ddl(self, outfile):
        r'''Writes trailing ';\n'
        '''
        outfile.write(
          f"CREATE SCHEMA {self.schema.name}{self.create_auth()};\n")

    def create_auth(self):
        r'''Returns auth string.

        Auth string starts with space and has no termination.
        '''
        if hasattr(self.schema, 'authorization'):
            return f" AUTHORIZATION {self.schema.authorization}"


class table:
    def __init__(self, schema, table):
        self.schema = schema
        self.table = table

    def create(self, outfile):
        outfile.write(
          f"CREATE TABLE{self.pre_name()} "
          f"{self.schema.sql_prefix()}{self.table.name}"
	  f"{self.post_name()} (\n")
        if hasattr(self.table, 'column') or hasattr(self.table, 'constraint'):
            if hasattr(self.table, 'column'):
                # no termination on final line:
                gen(partial(column, self), self.table.column, outfile,
                    separator=',\n')

                if hasattr(self.table, 'constraint'):
                    outfile.write(',\n')

                    # no termination on final line:
                    gen(partial(table_constraint, self),
                        self.table.constraint, outfile, separator=',\n')
            else:
                # no termination on final line:
                gen(partial(table_constraint, self),
                    self.table.constraint, outfile, separator=',\n')
        outfile.write('\n)')
        self.create_options(outfile)
        outfile.write(';\n')
        if hasattr(self.table, 'index'):
            outfile.write('\n')
            gen(partial(index, self), self.table.index, outfile, separator='')

    def pre_name(self):
        r'''Returns string with initial space, no termination.
        '''
        return ''

    def post_name(self):
        r'''Returns string with initial space, no termination.
        '''
        return ''

    def create_options(self, outfile):
        r'''Writes initial space, no termination.
        '''
        pass


class column:
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def create(self, outfile):
        r'''Writes lines indented 4 spaces with no termination.
        '''
        outfile.write(
          f"    {self.column.name} {sql_type(self).create()}")
        if hasattr(self.column, 'collate'):
            outfile.write(f" COLLATE {self.column.collate.upper()}")
        if asbool(self.column.primary_key):
            outfile.write(" PRIMARY KEY")
        if not asbool(self.column.nullable):
            outfile.write(" NOT NULL")
        if hasattr(self.column, 'links_to') and \
           asbool(self.column.check_foreign_key):
            outfile.write(f" REFERENCES {self.column.links_to}")


class sql_type:
    def __init__(self, column):
        self.column = column

    def create(self):
        r'''Returns a string with no leading or trailing spaces.
        '''
        return getattr(self, self.column.column.type.lower())()

    def integer(self):
        if hasattr(self.column.column, 'bit_size'):
            if int(self.column.column.bit_size) == 16:
                return "SMALLINT"
            if int(self.column.column.bit_size) == 32:
                return "INTEGER"
            elif int(self.column.column.bit_size) == 64:
                return "BIGINT"
            else:
                raise AssertionError(
                        f"{self.column.table.name}."
                        f"{self.column.column.name} has "
                        "unknown 'bit_size' value: "
                        f"{self.column.column.bit_size}")
        else:
            return "INTEGER"

    def float(self):
        if hasattr(self.column.column, 'bit_size'):
            if int(self.column.column.bit_size) == 32:
                return "REAL"
            elif int(self.column.column.bit_size) == 64:
                return "DOUBLE PRECISION"
            else:
                raise AssertionError(
                        f"{self.column.table.name}"
                        f".{self.column.column.name} has "
                        "unknown 'bit_size' value: "
                        f"{self.column.column.bit_size}")
        else:
            return "DOUBLE PRECISION"

    def decimal(self):
        if hasattr(self.column.column, 'num_digits'):
            if hasattr(self.column.column, 'num_decimals'):
                return f"DECIMAL({self.column.column.num_digits}, " \
                       f"{self.column.column.num_decimals})"
            else:
                return f"DECIMAL({self.column.column.num_digits})"
        elif hasattr(self.column.column, 'num_decimals'):
            raise AssertionError(
                    f"{self.column.table.name}"
                    f".{self.column.column.name} has "
                    "'num_decimals', so must also have 'num_digits'")
        return "DECIMAL"
    
    def boolean(self):
        return "BOOLEAN"

    def string(self):
        if hasattr(self.column.column, 'max_len'):
            return f"VARCHAR({self.column.column.max_len})"
        return "VARCHAR"

    def timestamp(self):
        if hasattr(self.column.column, 'fractional_second_digits'):
            ans = f"TIMESTAMP({self.column.column.max_len})"
        else:
            ans = "TIMESTAMP"
        if hasattr(self.column.column, 'with_time_zone') and \
           asbool(self.column.column.with_time_zone):
            return ans + ' WITH TIME ZONE'
        return ans


class table_constraint:
    def __init__(self, table, constraint):
        self.table = table
        self.constraint = constraint

    def create(self, outfile):
        getattr(self, self.constraint.type.lower())(outfile)

    def primary_key(self, outfile):
        outfile.write(
          f"    PRIMARY KEY ({', '.join(aslist(self.constraint.column))})")


class index:
    def __init__(self, table, index):
        self.table = table
        self.index = index

    def create(self, outfile):
        outfile.write(
          f"CREATE{self.unique()} INDEX {self.table.schema.sql_prefix()}"
          f"{self.table.table.name}__"
          f"{'__'.join(aslist(self.index.columns))}__idx\n")
        outfile.write(
          f"    ON {self.table.table.name}"
          f"({', '.join(aslist(self.index.columns))})")
        if hasattr(self.index, 'where'):
            outfile.write(f" WHERE {self.index.where}")
        outfile.write(';\n')

    def unique(self):
        if asbool(getattr(self.index, 'unique', 'false')):
            return " UNIQUE"
        return ''


def create(sql_gen, frame, outfile):
    #print("create", frame.class_name)
    getattr(sql_gen, frame.class_name.lower())(frame).create(outfile)



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
        db_conn = sql_gen_module.get_conn()
        user_id = db_conn.get_user_id('bruce')
        version_obj = db_conn.at_versions(user_id, *args.versions)
        create(sql_gen_module, version_obj.get_frame(args.frame_label),
               sys.stdout)
    else:
        raise AssertionError(f"Unknown command: {args.command}")
