# db.py

r'''Encapsulates the various python database modules.

Attempts to provide a unified interface to all implementations of the standard
python Database API (PEP 249, see https://www.python.org/dev/peps/pep-0249/).

It does this by presenting a single style of sql parameters that is then
translated into the sql parameter style expected by the specific python
database module in use.

This standard sql parameter style uses two forms of named sql parameters
within the sql string passed to the execute method.  The values for these
sql parameters are passed to the execute method as individual keyword
parameters (rather than a single mapping).  The two sql parameter forms are:

    - :name  -- for a single value

    - ::name -- for an iterable of values (used within parens in the SQL)
      - example: ... WHERE some_col IN (::values) ...
        with the execute keyword parameter: values=[1, 2, 3]

Also adds:

    - a default cursor to each connection so you don't have to create
      them yourself unless your doing nested sql calls.

    - the ability to use a connection as a contextmanager (in a python
      "with" statement) to commit/rollback at the end of the "with" block.
'''

from datetime import datetime
import re
from itertools import chain



class connection:
    r'''The unified version of the python database API connection class.

    Provides a default cursor.  The following methods map to that cursor:

        - __iter__
        - execute(*sql_lines, **param_values)
        - select(table_name, columns='*', **where)
        - select_1(table_name, columns='*', **where)
        - select_1_column(table_name, column, **where)
        - select_1_value(table_name, column, **where)
        - insert(table_name, **values)
        - insert_many(table_name, **values)
        - delete(table_name, **where)
        - update(table_name, where, **new_values)
        - fetchone()
        - fetchall()
        - lastrowid

    This can also be used as a contextmanager (in python "with" statement) to
    commit at the end of the "with" block.  This will rollback if the "with"
    block exits due to an uncaught exception.  It also captures the current
    system time (UTC) as 'self.now' at the start of the "with" block.
    '''
    def __init__(self, db, db_conn, trace=False):
        self.db = db
        self.db_conn = db_conn
        self.trace = trace
        self.default_cursor = self.cursor(self.trace)
        self.trans_attr_names = set()

    def reset_cursor(self):
        self.default_cursor.close()
        self.default_cursor = self.cursor(self.trace)

    def __getattr__(self, attr_name):
        return getattr(self.db_conn, attr_name)

    def set_trans_attr(self, name, value):
        #print("set_trans_attr setting", name, value)
        setattr(self, name, value)
        self.trans_attr_names.add(name)

    def del_trans_attr(self, name):
        #print("del_trans_attr deleting", name)
        delattr(self, name)
        self.trans_attr_names.remove(name)

    def trans_attrs(self, **attr_values):
        class context:
            def __enter__(self_c):
                for name, value in attr_values.items():
                    self.set_trans_attr(name, value)

            def __exit__(self_c, exc_type, exc_val, exc_tb):
                for name in attr_values.keys():
                    self.del_trans_attr(name)
                return False
        return context()

    def __enter__(self):
        self.default_cursor.execute('BEGIN')
        self.set_trans_attr('now', datetime.utcnow())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for attr in self.trans_attr_names:
            #print("__exit__ deleting", attr)
            delattr(self, attr)
        self.trans_attr_names = set()
        if exc_type is None and exc_val is None:
            self.commit()
        else:
            self.rollback()
        return False  # do not suppress exception

    def cursor(self, trace=False):
        return cursor(self, self.db_conn.cursor(), trace)

    def __iter__(self):
        return iter(self.default_cursor)

    def execute(self, *sql_lines, **params):
        return self.default_cursor.execute(*sql_lines, **params)

    def select(self, table_name, columns='*', **where):
        return self.default_cursor.select(table_name, columns, **where)

    def select_1(self, table_name, columns='*', **where):
        return self.default_cursor.select_1(table_name, columns, **where)

    def select_1_column(self, table_name, column, **where):
        return self.default_cursor.select_1_column(table_name, column, **where)

    def select_1_value(self, table_name, column, **where):
        return self.default_cursor.select_1_value(table_name, column, **where)

    def exists(self, table_name, **where):
        return self.default_cursor.exists(table_name, **where)

    def insert(self, table_name, **values):
        return self.default_cursor.insert(table_name, **values)

    def insert_many(self, table_name, **values):
        return self.default_cursor.insert_many(table_name, **values)

    def delete(self, table_name, **where):
        return self.default_cursor.delete(table_name, **where)

    def update(self, table_name, where, **new_values):
        return self.default_cursor.update(table_name, where, **new_values)

    def fetchall(self):
        return self.default_cursor.fetchall()

    def fetchone(self):
        return self.default_cursor.fetchone()

    @property
    def lastrowid(self):
        return self.default_cursor.lastrowid

    def rollback(self):
        if self.trace:
            print("//////// ROLLBACK " * 4, "////////", sep='')
        self.db_conn.rollback()

    def commit(self):
        if self.trace:
            print("COMMIT")
        self.db_conn.commit()

    def close(self):
        self.default_cursor.close()
        self.db_conn.close()


class cursor:
    r'''The unified version of the python database API cursor class.

    This execute method only accepts named parameters.  These are passed as
    individual keyword parameters; rather than as a single mapping parameter,
    as the python database API specifies.

    Adds the following helper methods:

        - select(table_name, columns='*', **where)
        - select_1(table_name, columns='*', **where)
        - select_1_column(table_name, column, **where)
        - select_1_value(table_name, column, **where)
        - insert(table_name, **values)
        - insert_many(table_name, **values)
        - delete(table_name, **where)
        - update(table_name, where, **new_values)

    Does not support executemany.
    '''

    paramstyles = {'qmark': ('?', "pos"),
                   'numeric': (':{}', "pos"),
                   'named': (':{}', "named"),
                   'format': ('%s', "pos"),
                   'pyformat': ('%({})s', "named"),
        }
    sql_param_re = re.compile(r':([a-zA-Z_][a-zA-Z0-9_]*)')
    sql_param_list_re = re.compile(r'::([a-zA-Z_][a-zA-Z0-9_]*)')

    def __init__(self, conn, db_cur, trace=False):
        self.connection = conn
        self.db_cur = db_cur
        self.sql_param, kind = self.paramstyles[self.connection.db.paramstyle]
        self.trace = trace
        if kind == "pos":
            self.execute = self.execute_pos
        else:
            self.execute = self.execute_named

    def __iter__(self):
        return iter(self.db_cur)

    def __getattr__(self, attr_name):
        return getattr(self.db_cur, attr_name)

    def execute_named(self, *sql_lines, **sql_params):
        sql = '\n'.join(sql_lines)
        if self.trace:
            print(sql)
            for name, value in sorted(sql_params.items()):
                print(f"{name}: {value}")
            print()
        new_params = sql_params.copy()
        def repl_list_fn(match):
            param_name = match.group(1)
            param_list = new_params.pop(param_name)
            ans = []
            for i, x in enumerate(param_list, 1):
                x_name = self.sql_param.format(f"param_name_{i}")
                new_params[x_name] = x
                ans.append(x_name)
            return ', '.join(ans)
        sql = self.sql_param_list_re.sub(repl_list_fn, sql)
        def repl_fn(match):
            return self.sql_param.format(match.group(1))
        try:
            self.db_cur.execute(self.sql_param_re.sub(repl_fn, sql), new_params)
        except self.connection.db.DatabaseError:
            print("SQL:", self.sql_param_re.sub(repl_fn, sql))
            raise

    def execute_pos(self, *sql_lines, **sql_params):
        sql = '\n'.join(sql_lines)
        if self.trace:
            print(sql)
            for name, value in sorted(sql_params.items()):
                print(f"{name}: {value}")
            print()
        param_num = 1
        new_params = []
        def repl_list_fn(match):
            nonlocal param_num
            param_name = match.group(1)
            param_list = sql_params[param_name]
            ans = []
            for x in param_list:
                x_name = self.sql_param.format(param_num)
                new_params.append(x)
                ans.append(x_name)
                param_num += 1
            return ', '.join(ans)
        sql = self.sql_param_list_re.sub(repl_list_fn, sql)
        def repl_fn(match):
            nonlocal param_num
            param_name = match.group(1)
            new_params.append(sql_params[param_name])
            ans = self.sql_param.format(param_num)
            param_num += 1
            return ans
        try:
            self.db_cur.execute(self.sql_param_re.sub(repl_fn, sql), new_params)
        except self.connection.db.DatabaseError:
            print("SQL:", self.sql_param_re.sub(repl_fn, sql))
            raise

    def select(self, table_name, columns='*', **where):
        r'''Use standard cursor commands to fetch rows.
        
        `colunns` is a comma separated string of column_names and/or
        sql expressions.  This is simply pasted in as SELECT {columns} FROM ...
        '''
        sql_lines, params = self.where(where, indent=1)
        self.execute(f"SELECT {columns}",
                     f"  FROM {table_name}",
                     *sql_lines,
                     **params)

    def select_1(self, table_name, columns='*', **where):
        r'''Returns one row.

        Raises AssertionError if not exactly one row found.
        '''
        self.select(table_name, columns, **where)
        i = 0
        for i, row in enumerate(self, 1):
            if i > 1:
                raise AssertionError(
                        f"More than one row found in {table_name}")
        if i:
            if self.trace:
                print("got", tuple(row))
            return row
        raise AssertionError(f"No row found in {table_name}")

    def select_1_column(self, table_name, column, **where):
        r'''Returns in iterable of the column values.
        '''
        self.select(table_name, column, **where)
        ans = (column for column, in self)
        if self.trace:
            ans = tuple(ans)
            print("got", ans)
        return ans

    def select_1_value(self, table_name, column, **where):
        r'''Returns one value from one row.

        No unpacking needed.
        '''
        value, = self.select_1(table_name, column, **where)
        if self.trace:
            print("got", value)
        return value

    def exists(self, table_name, **where):
        return bool(
                 next(
                   iter(
                     self.connection.select_1_column(table_name, "'True'",
                                                     **where)),
                   False))

    def where(self, exp, indent=0):
        r'''Takes dict of {col_name: value}; returns sql_lines, params.

        Checks for None to generate "IS NULL" test, and iterables to generate
        "IN (::param)" test.

        The returned sql_lines has "WHERE" on the first line, and "  AND" on
        the remaining lines.

        If `exp` is empty, no sql_lines or params are returned.
        '''
        sql_lines = []
        params = {}
        prefix = f"{' ' * indent}WHERE"
        for i, (col_name, value) in enumerate(exp.items()):
            if value is None:
                sql_lines.append(f"{prefix} {col_name} IS NULL")
            elif isiter(value):
                sql_lines.append(f"{prefix} {col_name} IN (::{col_name})")
                params[col_name] = value
            else:
                sql_lines.append(f"{prefix} {col_name} = :{col_name}")
                params[col_name] = value
            if i == 0:
                prefix = f"{' ' * indent}  AND"
        return sql_lines, params

    def insert(self, table_name, **values):
        self.execute(
               f"INSERT INTO {table_name} ({', '.join(values.keys())})",
               f"""VALUES ({', '.join(f":{key}" for key in values.keys())})""",
               **values)
        if self.trace:
            print("new", table_name, "id", self.lastrowid)

    def insert_many(self, table_name, **values):
        r'''Insert many rows into table_name.

        Iterable values are unpacked into multiple rows.  If more than one
        value is iterable, they must all have the same length.

        Singleton values are replicated across all rows.
        '''
        singleton_col_names = []
        multi_value_col_names = []
        select_columns = []
        num_rows = None
        params = {}
        for col_name, value in values.items():
            if isiter(value):
                multi_value_col_names.append(col_name)
                for i, v in enumerate(value, 1):
                    params[f"_{i}_{len(multi_value_col_names)}"] = v
                if num_rows is None:
                    num_rows = i
                elif num_rows != i:
                    raise ValueError(
                            f"Unequal number of rows in {col_name}, "
                            f"got {i}, others got {num_rows}")
            else:
                singleton_col_names.append(col_name)
                select_columns.append(f":{col_name}")
                params[col_name] = value

        col_names = ', '.join(chain(singleton_col_names, multi_value_col_names))
        one_row = ', '.join(f':_{{0}}_{col_num}'
                            for col_num
                             in range(1, len(multi_value_col_names) + 1))
        parened_row = f"({one_row})"
        rows = [parened_row.format(row_num)
                for row_num in range(1, num_rows + 1)]
        values = ',\n          '.join(rows)
        self.execute(f"INSERT INTO {table_name} ({col_names})",
                     f"SELECT {', '.join(select_columns)}, *",
                     f"  FROM (VALUES",
                     f"          {values})",
                     **params)

    def delete(self, table_name, **where):
        sql_lines, params = self.where(where, indent=1)
        self.execute(f"DELETE FROM {table_name}",
                     *sql_lines,
                     **params)

    def update(self, table_name, where, **new_values):
        sql_lines, params = self.where(where, indent=1)
        set_lines = []
        for col_name, new_value in new_values.items():
            param_name = col_name
            while param_name in params:
                param_name = f"_{param_name}"
            set_lines.append(f"{col_name} = :{param_name}")
            params[param_name] = new_value
        set = ',\n       '.join(set_lines)
        self.execute(f"UPDATE {table_name}",
                     f"   SET {set}",
                     *sql_lines,
                     **params)


def isiter(x):
    if isinstance(x, str):
        return False
    try:
        iter(x)
    except TypeError:
        return False
    return True


class db:
    r'''The unified version of the python database API module.

    Provides an optional post_connect function that is run on each new
    connection object.

    Can also use
    '''

    def __init__(self, db_module, connection=connection, post_connect=None):
        self.db_module = db_module
        self.post_connect = post_connect
        self.connection = connection

    def set_connection(self, conn):
        self.connection = conn

    def __getattr__(self, attr_name):
        return getattr(self.db_module, attr_name)

    def connect(self, *conn_params, trace=False, **conn_kws):
        conn = self.connection(self,
                               self.db_module.connect(*conn_params, **conn_kws),
                               trace)
        if self.post_connect is not None:
            self.post_connect(conn)
        return conn
