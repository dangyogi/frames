# frames_yaml.py

from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import users
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
        if 'users' in type:
            users.load_yaml(conn, type)
        elif 'versions' in type:
            versions.load_yaml(conn, type)
        elif 'frames' in type:
            frames.load_yaml(conn, type)
        else:
            raise ValueError(f"Did not find known key in {type.keys()}")



if __name__ == "__main__":
    import sys
    sys.setrecursionlimit(100)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db_trace', type=bool, default=False)
    parser.add_argument('--database', default='frames.db')
    parser.add_argument('--string')
    parser.add_argument('filename', nargs='?')
    args = parser.parse_args()

    if args.filename:
        import frames_db
        db_conn = frames_db.sqlite3_db().connect(args.database,
                                                 trace=args.db_trace)
        with db_conn:
            load_yaml(db_conn, args.filename)
    else:
        print(load_data(args.string))

