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

import re



class db:
    r'''The unified version of the python database API module.

    Provides an optional post_connect function that is run on each new
    connection object.

    Can also use
    '''

    def __init__(self, db_module, post_connect=None):
        self.db_module = db_module
        self.post_connect = post_connect
        self.connection = connection

    def set_connection(self, conn):
        self.connection = conn

    def __getattr__(self, attr_name):
        return getattr(self.db_module, attr_name)

    def connect(self, *conn_params, **conn_kws):
        conn = self.connection(self,
                               self.db_module.connect(*conn_params, **conn_kws))
        if self.post_connect is not None:
            self.post_connect(conn)
        return conn


class connection:
    r'''The unified version of the python database API connection class.

    Provides a default cursor.  The following methods map to that cursor:

        - __iter__
        - execute
        - fetchone
        - fetchall
        - lastrowid

    This can also be used as a contextmanager (in python "with" statement) to
    commit at the end of the "with" block.  This will rollback if the "with"
    block exits due to an uncaught exception.
    '''
    def __init__(self, db, db_conn):
        self.db = db
        self.db_conn = db_conn
        self.default_cursor = self.cursor()

    def reset_cursor(self):
        self.default_cursor.close()
        self.default_cursor = self.cursor()

    def __getattr__(self, attr_name):
        return getattr(self.db_conn, attr_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_value is None:
            self.rollback()
        else:
            self.commit()
        return False  # do not suppress exception

    def cursor(self):
        return cursor(self, self.db_conn.cursor())

    def __iter__(self):
        return iter(self.default_cursor)

    def execute(self, sql, **params):
        return self.default_cursor.execute(sql, **params)

    def fetchall(self):
        return self.default_cursor.fetchall()

    def fetchone(self):
        return self.default_cursor.fetchone()

    def lastrowid(self):
        return self.default_cursor.lastrowid

    def close(self):
        self.default_cursor.close()
        self.db_conn.close()


class cursor:
    r'''The unified version of the python database API cursor class.

    This execute method only accepts named parameters.  These are passed as
    individual keyword parameters; rather than as a single mapping parameter,
    as the python database API specifies.

    Does not support executemany.
    '''

    paramstyles = {'qmark': ('?', "pos"),
                   'numeric': (':{}', "pos"),
                   'named': (':{}', "named"),
                   'format': ('%s', "pos"),
                   'pyformat': ('%({})s', "named"),
        }
    sql_param_re = re.compile(r':([a-zA-Z][a-zA-Z0-9_]*)')
    sql_param_list_re = re.compile(r'::([a-zA-Z][a-zA-Z0-9_]*)')

    def __init__(self, conn, db_cur):
        self.connection = conn
        self.db_cur = db_cur
        self.sql_param, kind = self.paramstyles[self.connection.db.paramstyle]
        if kind == "pos":
            self.execute = self.execute_pos
        else:
            self.execute = self.execute_named

    def __iter__(self):
        return iter(self.db_cur)

    def __getattr__(self, attr_name):
        return getattr(self.db_cur, attr_name)

    def execute_named(self, sql, **sql_params):
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
        self.db_cur.execute(self.sql_param_re.sub(repl_fn, sql), new_params)

    def execute_pos(self, sql, **sql_params):
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
        self.db_cur.execute(self.sql_param_re.sub(repl_fn, sql), new_params)

