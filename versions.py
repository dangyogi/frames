# versions.py

import users


def get_version_id(conn, version_name):
    return conn.select_1_value("Version", "version_id", name=version_name)


def get_name(conn, version_id=None):
    if version_id is None:
        version_id = conn.version_id
    return conn.select_1_value("Version", "name", version_id=version_id)


def get_status(conn, version_id=None):
    if version_id is None:
        version_id = conn.version_id
    return conn.select_1_value("Version", "status", version_id=version_id)


def is_frozen(conn, version_id=None):
    if version_id is None:
        version_id = conn.version_id
    return get_status(conn, version_id) != 'proposed'


def load_yaml(conn, versions):
    r'''Caller must "with" conn prior to calling `load_yaml`.

    `versions` is the top-level dict for all version updates.
    '''
    with conn.trans_attrs(user_id=users.get_user_id(conn, versions['user'])):
        for section in versions['versions']:
            if 'add' in section:
                load_add_versions(conn, section['add'])
            elif 'change' in section:
                load_change_versions(conn, section['change'])
            elif 'delete' in section:
                load_delete_versions(conn, section['delete'])
            else:
                raise KeyError(f"Missing change-type in {section}")


def load_add_versions(conn, versions):
    for version in versions:
        #print("adding version", version)
        fields = version.copy()
        requires = fields.pop('requires', ())
        if 'status' in fields:
            raise AssertionError(
                    f"'status' not allowed in add version {fields['name']}")
        conn.insert("Version",
                    creation_user_id=conn.user_id,
                    creation_timestamp=conn.now,
                    **fields)
        id = conn.lastrowid
        print("adding version", fields['name'], "new id", id)
        if requires:
            subsets = set(conn.select_1_column("Version", "version_id",
                                               name=requires))
            #print("requires ids", sorted(subsets))
            conn.insert_many("Version_requires",
                             version_id=id,
                             required_version_id=subsets,
                             creation_user_id=conn.user_id,
                             creation_timestamp=conn.now)

            subsets.update(conn.select_1_column("Version_subsets", "subset_id",
                                                superset_id=subsets))

            print(" ", "subsets", sorted(subsets), "for superset", id)

            conn.insert_many("Version_subsets", superset_id=id,
                             subset_id=subsets)


def load_change_versions(conn, changes):
    for change in changes:
        if len(change) != 1:
            raise AssertionError(
                    f"Only one version per change allowed {change.keys()}")
        for name, fields in change.items():
            current_status = conn.select_1_value("Version", "status", name=name)
            #if 'requires' in fields:
            #    requires = fields.pop('requires')
            #    if requires and current_status == 'final':
            #        raise AssertionError(
            #                "Can't change required versions when status is "
            #                "final")
            #else:
            #    requires = None

            new_values = {}
            for col_name, value in fields.items():
                if col_name in (
                  'version_id', 'requires',
                  'creation_user_id', 'creation_timestamp',
                  'updated_user_id', 'updated_timestamp'):
                    raise AssertionError(
                            f"Can't change {col_name} in version {name!r}")
                if col_name == 'status' and current_status == 'final':
                    if value != 'final':
                        raise AssertionError(
                                "Can not change final status in version "
                                f"{name}")
                else:
                    new_values[col_name] = value

            if new_values:
                conn.update("Version", dict(name=name),
                            updated_user_id=conn.user_id,
                            updated_timestamp=conn.now,
                            **new_values)

            #if requires:
            #    conn.execute(
            #           "SELECT version_id FROM Version WHERE name = :name",
            #           name=name)
            #    id = conn.fetchone()[0]
            #    if 'add' in requires:
            #        conn.execute("""
            #              INSERT INTO Version_requires
            #                       (version_id, required_version_id,
            #                        creation_user_id, creation_timestamp)
            #              SELECT :id, version_id, :user_id, :now
            #                FROM Version
            #               WHERE name in (::add)""",
            #            id=id, add=requires['add'], user_id=user_id, now=now)
            #    if 'delete' in requires:
            #        conn.execute("""
            #              DELETE FROM Version_requires
            #              WHERE version_id = :id
            #                AND required_version_id IN (
            #                      SELECT version_id
            #                        FROM Version
            #                       WHERE name IN (::delete))""",
            #            id=id, delete=requires['delete'])


def load_delete_versions(conn, names):
    conn.delete('Version', name=names)


class version_obj:
    def __init__(self, user_id, version_name):
        pass
