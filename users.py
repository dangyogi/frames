# users.py


__all__ = ('get_user', 'get_user_id', 'load_yaml')



def get_user(conn, login):
    return conn.select_1("User", login=login)


def get_user_id(conn, login):
    return get_user(conn, login)['user_id']


def load_yaml(conn, users):
    r'''Caller must do commit on conn.

    `users` is the top-level dict for all user updates.
    '''
    for section in users['users']:
        if 'add' in section:
            load_add_users(conn, section['add'])
        elif 'change' in section:
            load_change_users(conn, section['change'])
        elif 'delete' in section:
            load_delete_users(conn, section['delete'])
        else:
            raise KeyError(f"Missing change-type in {section}")


def load_add_users(conn, users):
    for user in users:
        fields = user.copy()
        if 'email' not in fields:
            fields['email'] = None
        conn.insert('User', **fields)


def load_change_users(conn, changes):
    for change in changes:
        assert len(change) == 1, \
               f"Only one user per change allowed {change.keys()}"
        for login, fields in change.items():
            assert 'user_id' not in fields
            conn.update('User', dict(login=login), **fields)


def load_delete_users(conn, logins):
    conn.delete('User', login=logins)

