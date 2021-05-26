# frames_yaml.py

from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import versions
import frames


def load_data(s):
    return load(s, Loader=Loader)

def read_file(filename):
    with open(filename, 'r') as file:
        return load(file, Loader=Loader)

def load_yaml(conn, filename):
    r'''Caller must use `conn` within "with" statement.
    '''
    for type in read_file(filename):
        if 'versions' in type:
            versions.load_yaml(conn, type)
        elif 'frames' in type:
            frames.load_yaml(conn, type)
        else:
            raise ValueError(f"Did not find known key in {type.keys()}")



if __name__ == "__main__":
    import sys
    sys.setrecursionlimit(100)

    import argparse

    parser = argparse.ArgumentParser(description="Load yaml/parse yaml")
    parser.add_argument('--reset_db', action='store_true', default=False)
    parser.add_argument('--schema', default='frame_schema.sql')
    parser.add_argument('--db_trace', action='store_true', default=False)
    parser.add_argument('--database', default='frames.db')
    parser.add_argument('--string')
    parser.add_argument('filename', nargs='?')
    args = parser.parse_args()

    #print("args.reset_db", args.reset_db)
    #print("args.schema", args.schema)
    #print("args.db_trace", args.db_trace)
    #print("args.database", args.database)
    #print("args.string", args.string)
    #print("args.filename", args.filename)

    if args.filename:
        import os
        import frames_db
        if args.reset_db:
            if os.path.exists(args.database):
                print("Removing", args.database)
                os.remove(args.database)
        db_conn = frames_db.sqlite3_db().connect(args.database,
                                                 trace=args.db_trace)
        if args.reset_db:
            with open(args.schema, 'r') as sql, db_conn:
                print("Loading", args.schema)
                db_conn.executescript(sql.read())
            print("Loaded")
        with db_conn:
            print("Loading", args.filename)
            load_yaml(db_conn, args.filename)
    else:
        print("--string", args.string)
        print(load_data(args.string))

