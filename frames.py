# frames.py

import re
from itertools import groupby, chain
from operator import itemgetter
from collections import defaultdict

from jinja2 import Environment, StrictUndefined, exceptions


Jinja2_env = Environment(trim_blocks=True,
                         lstrip_blocks=True,
                         keep_trailing_newline=True,
                         autoescape=False,
                        #variable_start_string='{',
                        #variable_end_string='}',
                         undefined=StrictUndefined,
                        )

def format(templ, context):
    return Jinja2_env.from_string(templ).render(context)


class db:
    r'''Encapsulates the various database modules.

    Provides a unified interface to the various forms of sql parameter passing.

    This uses two forms of named sql params:
        - :name  -- for a single value
        - ::name -- for an iterable of values (used within parens in the SQL)
          - example: ... WHERE some_col IN (::values) ...
            with the sql_param: values=[1, 2, 3]

    This executes all sql statements on the same cursor.

    The only connection methods availabe are commit(), rollback() and close().

    The only cursor method available is to treat the db as an iterator,
    which iterates on the cursor, fetchall(), fetchone(), and lastrowid().
    '''
    paramstyles = {'qmark': ('?', "pos"),
                   'numeric': (':{}', "pos"),
                   'named': (':{}', "named"),
                   'format': ('%s', "pos"),
                   'pyformat': ('%({})s', "named"),
        }
    sql_param_re = re.compile(r':([a-zA-Z][a-zA-Z0-9_]*)')
    sql_param_list_re = re.compile(r'::([a-zA-Z][a-zA-Z0-9_]*)')

    def __init__(self, module, *conn_params, post_connect=None, **conn_kws):
        self.module = module
        self.conn = self.module.connect(*conn_params, **conn_kws)
        if post_connect is not None:
            post_connect(self.conn)
        self.cursor = self.conn.cursor()
        self.sql_param = self.paramstyles[self.module.paramstyle][0]
        if self.paramstyles[self.module.paramstyle][1] == "pos":
            self.execute = self.execute_pos
        else:
            self.execute = self.execute_named

    def at_versions(self, user_id, *version_names):
        return version(self, user_id, *version_names)

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
        self.cursor.execute(self.sql_param_re.sub(repl_fn, sql), new_params)

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
        self.cursor.execute(self.sql_param_re.sub(repl_fn, sql), new_params)

    def __iter__(self):
        return iter(self.cursor)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def lastrowid(self):
        return self.cursor.lastrowid

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()


class version:
    def __init__(self, db, user_id, *version_names):
        self.db = db
        self.user_id = user_id
        self.version_names = version_names
        self.lookup_version_ids()
        self.required_versions, self.required_map = \
          self.get_all_required_versions()
        print("version_obj", self.version_names, self.version_ids, self.required_versions)

    def lookup_version_ids(self):
        self.db.execute("""SELECT version_id, status FROM Version
                            WHERE name_upper IN (::version_names)""",
                        version_names=[v.upper() for v in self.version_names])
        version_ids = []
        self.frozen = True
        for row in self.db:
            version_ids.append(row[0])
            if row[1] == 'proposed':
                self.frozen = False
        self.version_ids = frozenset(version_ids)

    def get_all_required_versions(self, seen=None, depth=0):
        r'''Figures out all of the required version info.

        Returns ({required_version_id}, {version_id: set(required_version_ids)})
        '''
        self.db.execute("""WITH RECURSIVE req(ver_id, req_ver_id) AS (
                         SELECT version_id, required_version_id
                           FROM version_requires
                          WHERE version_id in (::version_ids)
                       UNION ALL
                         SELECT version_id, required_version_id
                           FROM version_requires
                                INNER JOIN req
                          WHERE version_id == req_ver_id
                       )

                       SELECT ver_id, req_ver_id FROM req
                        ORDER BY ver_id;""",
                    version_ids=self.version_ids)
        required_map = {version_id: set(req_ver_id
                                        for ver_id, req_ver_id
                                         in required_versions)
                        for version_id, required_versions
                         in groupby(self.db, key=itemgetter(0))}
        #print("required_map", required_map)
        def fill_req(req_versions, remaining_req_versions):
            for req_ver in remaining_req_versions:
                if req_ver in required_map:
                    deeper_req_versions = \
                      required_map[req_ver].difference(req_versions)
                    req_versions.update(deeper_req_versions)
                    fill_req(req_versions, deeper_req_versions)
        for ver_id, req_versions in required_map.items():
            fill_req(req_versions, req_versions.copy())

        all_required = set(self.version_ids)
        for version_id, req_set in required_map.items():
            all_required.add(version_id)
            all_required.update(req_set)

        return all_required, required_map

    def frame_ids_with_slots(self, **slots):
        r'''Searches for all frames that have all of the indicated slots.

        Only does == matches.  String compares are case insensitive.

        Returns a set of frame_ids.
        '''
        slot_names = frozenset(k.upper() for k in slots.keys())
        slot_names_with_ako = slot_names.union(['AKO'])
        slots_found = self.select_slots_by_version(
                        "name_upper IN (::slot_names_with_ako)",
                        slot_names_with_ako=slot_names_with_ako)

        # {base_id: {derived_id}}
        derived_map = defaultdict(set)
        for (frame_id, name, _), slot in slots_found.items():
            if name == 'AKO':
                derived_map[slot['value']].add(frame_id)

        def frames_with_slot(slot_name, value):
            r'''Slot_name is passed in uppercase.
            '''
            if isinstance(value, frame):
                value = frame.frame_id
            if isinstance(value, str):
                value = value.upper()
            for (frame_id, name, value_order), slot in slots_found.items():
                if name == slot_name and \
                   value in ((slot['value'].upper()
                              if isinstance(slot['value'], str)
                              else slot['value']),
                             '*'):
                    yield frame_id
                    if slot_name != 'FRAME_NAME':
                        yield from spew_derived(frame_id, name, value_order)

        def spew_derived(frame_id, name, value_order):
            for d in derived_map[frame_id]:
                if (d, name, value_order) not in slots_found:
                    yield d
                    yield from spew_derived(d, name, value_order)

        found = [frozenset(frames_with_slot(slot_name.upper(), value))
                 for slot_name, value in slots.items()]
        return found[0].intersection(*found[1:])

    def get_raw_frame(self, frame_label):
        r'''Reads one frame from the database.
        
        frame_label can be either a frame_id (either int or str),
        or a frame_name.

        Only includes the proper slots to use for this set of versions.

        Does not include inherited slots.
        
        Returns frame_id, {(frame_id, name.upper(), value_order): slot}

        Where slot is dict with the following keys:
            - frame_id
            - slot_id
            - name
            - value_order
            - description
            - value
        '''
        if isinstance(frame_label, int) or \
           isinstance(frame_label, str) and frame_label.isdigit():
            frame_id = int(frame_label)
        else:
            raw_frame = self.select_slots_by_version(
                          'name_upper = "FRAME_NAME" AND upper(value) = :name',
                          name=frame_label.upper())
            frame_id = next(iter(raw_frame))[0]
        return (frame_id,
                self.select_slots_by_version("frame_id = :frame_id",
                                             frame_id=frame_id))

    def select_slots_by_version(self, where_exp, **sql_params):
        r'''Figures slots matching where_exp/sql_params that are best match to
        my versions.

        Returns a raw_frame.
        '''
        self.db.execute(f"""
                        SELECT frame_id, name, value_order, slot_id, version_id
                          FROM Slot
                               INNER JOIN Slot_versions USING (slot_id)
                         WHERE {where_exp}
                         ORDER BY frame_id, name, value_order, slot_id;""",
                        **sql_params)

        matching_slot_ids = self.select_slot_ids_by_version(self.db)

        self.db.execute("""SELECT *
                             FROM Slot
                            WHERE slot_id IN (::slot_ids)
                            ORDER BY frame_id, name_upper, value_order;""",
                        slot_ids=matching_slot_ids)

        return {(row['frame_id'], row['name'].upper(), row['value_order']):
                dict(frame_id=row['frame_id'],
                     slot_id=row['slot_id'],
                     name=row['name'],
                     value_order=row['value_order'],
                     description=row['description'],
                     value=row['value'])
                for row in self.db}

    def select_slot_ids_by_version(self, raw_slot_rows):
        r'''raw_slot_rows is (frame_id, name, value_order, slot_id, version_id)

        raw_slots must be sorted by frame_id, upper(name), value_order, slot_id.

        Returns a list of selected slot_ids.
        '''
        matching_slot_ids = []
        for (frame_id, name, value_order), slots \
         in groupby(raw_slot_rows, key=itemgetter(0, 1, 2)):
            matching_slots = []  # [(slot_id, version_ids_frozenset)]
            for slot_id, versions in groupby(slots, key=itemgetter(3)):
                version_ids = frozenset(v[4] for v in versions)
                if version_ids.issubset(self.required_versions):
                    matching_slots.append((slot_id, version_ids))

            #print("matching_slots", matching_slots)
            if len(matching_slots) == 1:
                matching_slot_ids.append(matching_slots[0][0])
            else:
                best_match = None  # (slot_id, versions)
                for slot_id, versions in matching_slots:
                    #print("checking", slot_id, versions)
                    for slot_id2, versions2 in matching_slots:
                        if slot_id != slot_id2 and \
                           not self.better_fit(slot_id, versions,
                                               slot_id2, versions2):
                            # nope, not this one!
                            break
                    else:
                        if best_match is not None:
                            # Conflict!
                            raise AssertionError(
                                    "Slot version conflict between "
                                    f"{best_match[0]} and {slot_id}")
                        else:
                            best_match = (slot_id, versions)
                if best_match is not None:
                    matching_slot_ids.append(best_match[0])
        return matching_slot_ids

    def better_fit(self, slot_id, versions, other_slot_id, other_versions):
        #print("better_fit", versions, other_versions)
        if len(other_versions) > len(versions):
            # We'll catch any version conflicts when the two versions are
            # checked in the reverse order...
            #print("better_fit -> False, len(other_versions) > len(versions)")
            return False
        num_better = 0
        num_worse = 0
        num_matches = 0
        for v in versions:
            num_matches = 0
            for other_v in other_versions:
                if v == other_v:
                    num_matches += 1
                elif v in self.required_map and other_v in self.required_map[v]:
                    # v is better than other_v
                    num_better += 1
                elif other_v in self.required_map and \
                     v in self.required_map[other_v]:
                    # other_v is better than v
                    num_worse += 1
        #print("better_fit", "num_worse", num_worse, "num_better", num_better,
        #      "num_matches", num_matches)
        if num_worse and num_better or \
           num_worse and len(versions) > len(other_versions) or \
           num_better + num_worse + num_matches < len(other_versions) or \
           num_matches == len(other_versions) == len(versions):
            raise AssertionError(
                    f"Slot version conflict between {other_slot_id} and "
                    f"{slot_id}")
        if num_worse:
            #print("better_fit -> False, num_worse", num_worse)
            return False
        ans = num_better or len(versions) > len(other_versions)
        #print("better_fit ->", ans, "final")
        return ans

    def with_inherited_slots(self, frame_id, raw_frame):
        r'''Returns a new raw_frame that includes its inherited slots.

        Returns same structure as get_raw_frame.
        '''
        #print("with_inherited_slots", frame_id)
        ako = raw_frame.get((frame_id, 'AKO', None))
        if ako and ako['value'][0] == '$':
            base_frame_id, base_frame = self.get_raw_frame(ako['value'][1:])
            return derive(raw_frame,
                          self.with_inherited_slots(base_frame_id, base_frame))
        return raw_frame

    def get_frame(self, frame_label, format_slots=True):
        r'''Returns a frame object.

        Includes inherited slots.

        Reads in all sub-frames.
        '''
        frame_id, raw_frame = self.get_raw_frame(frame_label)
        return frame(frame_id, self,
                     self.with_inherited_slots(frame_id, raw_frame),
                     format_slots=format_slots)

    def update_slot(self, slot_id, value, description=None):
        r'''Returns slot_id (may have had to create a new one)
        '''
        if self.frozen:
            raise AssertionError("Can not make changes to frozen versions")

        self.db.execute("""SELECT version_id FROM Slot_versions
                            WHERE slot_id = :slot_id""",
                        slot_id=slot_id)
        slot_versions = frozenset(row[0] for row in self.db)
        if slot_versions == self.version_ids:
            if isinstance(value, frame):
                if hasattr(value, 'frame_name'):
                    db_value = f"${value.frame_name}"
                else:
                    db_value = f"${value.frame_id}"
            else:
                db_value = value
            self.db.execute("""
              UPDATE Slot
                 SET value = :value, description = :description,
                     updated_user_id = :user_id,
                     updated_timestamp = datetime("now")
               WHERE slot_id = :slot_id""",
              value=value, description=description, slot_id=slot_id,
              user_id=self.user_id)
            return slot_id
        self.db.execute("""SELECT frame_id, name, value_order
                             FROM Slot
                            WHERE slot_id = :slot_id""",
                        slot_id=slot_id)
        frame_id, name, value_order = self.db.fetchone()
        raw_slot = self.create_slot(frame_id, name, value_order, value,
                                    description)
        return raw_slot['slot_id']

    def create_slot(self, frame_id, name, value_order, value, description=None):
        r'''Returns a raw_slot (see get_raw_frame for what a "raw_slot" is).
        '''
        if self.frozen:
            raise AssertionError("Can not make changes to frozen versions")
        if isinstance(value, frame):
            if hasattr(value, 'frame_name'):
                db_value = f"${value.frame_name}"
            else:
                db_value = f"${value.frame_id}"
        else:
            db_value = value
        # Insert the new slot row
        self.db.execute("""
          INSERT INTO Slot (frame_id, name, name_upper, value_order,
                            description, value,
                            creation_user_id, creation_timestamp)
          VALUES (:frame_id, :name, :name_upper, :value_order, :description,
                  :value, :creation_user_id, datetime("now"));""",
          frame_id=frame_id, name=name, name_upper=name.upper(),
          value_order=value_order, description=description, value=db_value,
          creation_user_id=self.user_id)
        slot_id = self.db.lastrowid()

        # Assign version_ids to new slot
        for version_id in self.version_ids:
            self.db.execute("""
              INSERT INTO Slot_versions (slot_id, version_id,
                                         creation_user_id, creation_timestamp)
              VALUES (:slot_id, :version_id, :creation_user_id, datetime("now"));""",
              slot_id=slot_id, version_id=version_id,
              creation_user_id=self.user_id)

        return dict(frame_id=frame_id,
                    slot_id=slot_id,
                    name=name,
                    value_order=value_order,
                    description=description,
                    value=value)

    def create_frame(self, slots):
        r'''Creates a new frame with the slots specified.

        `slots` is {name: value}.

        The value_order of any list/tuple values are assigned starting at 1.

        Only called by load_yaml/load_frame.

        Returns the frame_label ("$<frame_id>" or "$<frame_name>") for the new
        frame.
        '''
        self.db.execute("""SELECT frame_id FROM Slot
                            ORDER BY frame_id DESC
                            LIMIT 1""")
        rows = self.db.fetchall()
        if rows:
            assert len(rows) == 1
            frame_id = rows[0][0] + 1
        else:
            frame_id = 1

        def unwrap_value(slot_name, value, version_obj=self,
                         current_index=None, value_order_offset=None):
            r'''A "value_info" object may stand in for a single value.

            This is a dict with the following keys:
                - value             -- the value it's standing in for (required)
                - user_id           -- the user_id to store in creation_user_id
                - required_versions -- list of version names for slot_versions
                - value_order       -- the offset to apply to value_order
                                       within a list
            '''
            new_offset = None
            while isinstance(value, dict) and 'value' in value:  # value info
                user_id = None
                required_versions = None
                for key, info in value.items():
                    key_upper = key.upper()
                    if key_upper == 'VALUE_ORDER':
                        new_offset = info
                    elif key_upper == 'USER_ID':
                        user_id = info
                    elif key_upper == 'REQUIRED_VERSIONS':
                        required_versions = info
                    elif key_upper != 'VALUE':
                        raise AssertionError(
                                f"Unknown key, {key}, on slot {slot_name}")
                if user_id is not None or required_versions is not None:
                    version_obj = self.db.at_versions(
                                    user_id or self.user_id,
                                    *(required_versions
                                        if required_versions is not None
                                        else self.version_names))
                value = value['value']
            if current_index is None and new_offset is not None:
                raise AssertionError(
                        "value_order not allowed on single-valued "
                        f"slot {slot_name}")
            if isinstance(value, dict):   # nested frame
                value = version_obj.create_frame(value)
            if new_offset is not None:
                return version_obj, new_offset - i, value
            return version_obj, value_order_offset, value
        frame_label = f"${frame_id}"
        for name, value in slots.items():
            print("create_frame", name, value)
            version_obj, _, value = unwrap_value(name, value)
            if not isinstance(value, (list, tuple)):
                if name.upper() == 'FRAME_NAME':
                    frame_label = f"${value}"
                version_obj.create_slot(frame_id, name, None, value)
            else:
                if name.upper() in ("ISA", "AKO", "FRAME_NAME"):
                    raise AssertionError(
                            f"{name} slot not allowed to have multiple values")
                value_order_offset = 1
                for i, v in enumerate(value):
                    this_version_obj, value_order_offset, v = \
                      unwrap_value(name, v, version_obj, i, value_order_offset)
                    this_version_obj.create_slot(frame_id, name,
                                                 i + value_order_offset,
                                                 v)
        return frame_label


def derive(derived, base):
    r'''Returns new raw frame deriving `derived` from `base`.

    Both `derived` and `base` are raw frames (e.g., returned from
    get_raw_frame).
    '''
    #print("derive")
    ans = {}
    derived_keys = frozenset((name, value_order)
                             for _, name, value_order in derived.keys())
    for base_key, slot in base.items():
        if base_key[1:] not in derived_keys:
            # This test is probably not necessary... ???
            if slot['name'].upper() != 'FRAME_NAME' and \
               slot['value'].upper() != '<DELETED>':
                #print("taking", base_key, "from base")
                ans[base_key] = slot
    #print("from base", ans)
    for key, slot in derived.items():
        if slot['name'].upper() != 'AKO':
            #print("taking", key, "from derived")
            ans[key] = slot
    return ans


class frame:
    r'''Interface object for a frame.

    some_frame.slot_name -> value (may be a slot_list) # but not '<deleted>'
    some_frame.get_raw_slot(slot_name)  # see get_raw_frame for raw slots
                                        # doesn't hide '<deleted>'
    some_frame.get_slot_names() -> iterable of slot_names (excluding <deleted>)
    '''
    def __init__(self, frame_id, version_obj, raw_frame, format_slots=True):
        # For raw_frame structure, see get_raw_frame
        self.frame_id = frame_id
        self.version_obj = version_obj
        self.raw_slots = {}  # {name.upper(): raw_slot|slot_list}
        for name, slots_by_name in groupby(sorted(raw_frame.items(),
                                                  key=lambda item: item[0][1:]),
                                           key=lambda item: item[0][1]):
            #print("frame.__init__", frame_id, name)
            first_slot = next(slots_by_name)[1]
            if first_slot['value_order'] is None:
                self.raw_slots[name] = first_slot
                next_slot = next(slots_by_name, 'empty')
                if next_slot != 'empty':
                    raise AssertionError(
                            "Got value_order of None in multi-valued slot: "
                            f"slot_id {first_slot['slot_id']}, "
                            f"next_slot {next_slot[1]['slot_id']}")
            else:
                #print("frame got multi-value", name)
                self.raw_slots[name] = slot_list(self, name,
                                             chain([first_slot],
                                                   (item for key, item
                                                          in slots_by_name)))
        self.fill_frame()
        if format_slots:
            self.format_slots()
    
    def __repr__(self):
        if hasattr(self, 'name'):
            if hasattr(self, 'isa'):
                return f"<frame({self.frame_id}): {self.isa}({self.name})>"
            return f"<frame({self.frame_id}): {self.name}>"
        if hasattr(self, 'isa'):
            return f"<frame({self.frame_id}): {self.isa}>"
        return f"<frame({self.frame_id})>"

    def frozen(self):
        return self.version_obj.frozen

    def get_slot_names(self):
        r'''The returned names have been uppercased.
        '''
        return [key for key, slot in self.raw_slots.items()
                    if isinstance(slot, slot_list) or
                       slot['value'].upper() != '<DELETED>']

    def __getattr__(self, slot_name):
        r'''`slot_name` can be any case (upper/lower).
        '''
        slot = self.get_raw_slot(slot_name)
        if isinstance(slot, slot_list):
            return slot
        if slot['value'].upper() == '<DELETED>':
            raise AttributeError(f"{slot_name} deleted")
        return slot['value']

    def get_raw_slot(self, slot_name):
        r'''`slot_name` can be any case (upper/lower).
        '''
        try:
            return self.raw_slots[slot_name.upper()]
        except KeyError:
            raise AttributeError(slot_name)

    def as_raw_frame(self):
        ans = {}
        for name, raw_slot in self.raw_slots.items():
            if isinstance(raw_slot, slot_list):
                raw_slot.populate_raw_frame(ans)
            else:
                ans[self.frame_id, name, None] = raw_slot
        return ans

    def print(self):
        for name in sorted(self.get_slot_names()):
            print(name, getattr(self, name))

    def dump(self, indent=0):
        for name in sorted(self.get_slot_names()):
            value = getattr(self, name)
            print(' ' * indent, end='')
            if isinstance(value, frame):
                print(f"{name}:")
                value.dump(indent + 2)
            elif isinstance(value, slot_list):
                print(f"{name}: [")
                need_sep = False
                for v in value:
                    if need_sep:
                        print()
                    if isinstance(v, frame):
                        v.dump(indent + 2)
                        need_sep = True
                    else:
                        print(' ' * (indent + 2), v, ',', sep='')
                        need_sep = False
                print(' ' * indent, "]", sep='')
            else:
                print(name, value)

    def insert_raw_slot(self, name, raw_slot):
        r'''This can override an existing raw_slot.
        '''
        # FIX: What happens if name is "ako"??
        if name.upper() in self.raw_slots and \
           isinstance(self.raw_slots[name.upper()], slot_list):
            # This may never get used...
            self.raw_slots[name.upper()].insert_raw_slot(raw_slot)
        else:
            self.raw_slots[name.upper()] = raw_slot

    def delete_raw_slot(self, name, value_order, description=None):
        # FIX: What happens if name is "ako"??
        self.set_raw_slot(self, name, value_order, 'delete', None, description)

    def set_raw_slot(self, name, value_order, value, description=None):
        r'''This can override an existing raw_slot.
        '''
        # FIX: What happens if name is "ako"??
        if name.upper() in self.raw_slots:
            raw_slot = self.raw_slots[name.upper()]
            if isinstance(raw_slot, slot_list):
                # This may never get used...
                raw_slot.set_raw_slot(value_order, value, description)
            elif raw_slot['frame_id'] != self.frame_id or \
                 value_order is not None:
                self.create_raw_slot(name, value_order, value, description)
            else:
                slot_id = self.version_obj.update_slot(raw_slot['slot_id'],
                                                       value=value,
                                                       description=description)
                raw_slot['slot_id'] = slot_id
                raw_slot['value'] = value
                raw_slot['description'] = description
        else:
            self.create_raw_slot(name, value_order, value, description)

    def create_raw_slot(self, name, value_order, value, description=None):
        # FIX: What happens if name is "ako"??
        slot_id = self.version_obj.create_slot(self.frame_id, name,
                                               value_order, value, description)
        if value_order is None:
            self.raw_slots[name.upper()] = raw_slot
        else:
            self.raw_slots[name.upper()] = slot_list(self, name, [raw_slot])

    def fill_frame(self):
        r'''Gets all frames referred to by "$frame" slots.

        Replaces the "value" (was the frame_id) with the entire frame.

        Also splices multi-valued slots where the sub-frame "isa" "splice".

        Does of all this in place.  Returns nothing.
        '''
        #print("fill_frame", self.frame_id)
        for name, slot in self.raw_slots.items():
            #print("fill_frame", name, slot)
            if isinstance(slot, slot_list):
                #print("got slot_list")
                i = 0
                while i < len(slot):
                    raw_slot = slot.get_raw_slot(i)
                    #print("index", i, raw_slot)
                    if raw_slot['value'][0] == '$':
                        sub_frame = \
                          self.version_obj.get_frame(raw_slot['value'][1:],
                                                     False)
                        #print("sub_frame ", end='')
                        #sub_frame.print()
                        if getattr(sub_frame, 'isa', None) == 'splice':
                            #print("sub_frame is splice")
                            new_slots = slot.splice(i, sub_frame)
                            i += len(new_slots)
                        else:
                            raw_slot['value'] = sub_frame
                            #print("sub_frame not splice", raw_slot)
                            i += 1
                    else:
                        i += 1
                #print("final slot_list", slot)
            elif isinstance(slot['value'], str) and slot['value'][0] == '$':
                #print("not slot_list")
                sub_frame = self.version_obj.get_frame(slot['value'][1:], False)
                if getattr(sub_frame, 'isa', None) == 'splice':
                    raise AssertionError(
                            f"slot_id {slot['slot_id']} with null value_order "
                            "points to splice")
                else:
                    slot['value'] = sub_frame

    def format_slots(self, context=None):
        if context is None:
            context = {}
        if hasattr(self, 'isa'):
            context[self.isa] = self
        context['frame'] = self
        def format_slot(raw_slot):
            if isinstance(raw_slot, slot_list):
                for list_raw_slot in raw_slot.iter_raw_slots():
                    format_slot(list_raw_slot)
            else:
                if isinstance(raw_slot['value'], frame):
                    raw_slot['value'].format_slots(context)
                elif isinstance(raw_slot['value'], str) and \
                     '{{' in raw_slot['value']:
                    try:
                        #raw_slot['value'] = raw_slot['value'].format(**context)
                        raw_slot['value'] = format(raw_slot['value'], context)
                    except (AttributeError, exceptions.UndefinedError):
                        # assume this frame is designed to only be used as ako
                        # where derived frame defines what's needed in the
                        # format.
                        pass
        for raw_slot in self.raw_slots.values():
            format_slot(raw_slot)


class slot_list:
    r'''Interface to lists of values (stored as multiple slots)

    some_slot_list[i]
    some_slot_list.get_raw_slot(i)
    some_slot_list.splice(i, splice_frame)
    some_slot_list.iter_raw_slots()
    some_slot_list.insert_raw_slot(raw_slot)

    Also supports len() and iteration.
    '''
    def __init__(self, frame, name, raw_slots):
        self.frame = frame
        self.name = name
        self.raw_slots = list(raw_slots)
        #print("slot_list", self.raw_slots)

    def __str__(self):
        return f"[{', '.join(str(v['value']) for v in self.raw_slots)}]"

    def __getitem__(self, i):
        return self.raw_slots[i]['value']

    def __len__(self):
        return len(self.raw_slots)

    def iter_raw_slots(self):
        return self.raw_slots

    def __iter__(self):
        for v in self.raw_slots:
            yield v['value']

    def get_raw_slot(self, i):
        return self.raw_slots[i]

    def populate_raw_frame(self, raw_frame):
        for raw_slot in self.raw_slots:
            key = (self.frame.frame_id, raw_slot['name'].upper(),
                   raw_slot['value_order'])
            raw_frame[key] = raw_slot

    def insert_raw_slot(self, raw_slot):
        value_order = raw_slot['value_order']
        if value_order is None:
            raise AssertionError(
                    f'Inserted multi-value slot {raw_slot["slot_id"]} ' \
                    'has no "value_order"')
        for i, my_raw_slot in enumerate(self.raw_slots):
            my_value_order = my_raw_slot['value_order']
            if my_value_order == value_order:
                self.raw_slots[i] = raw_slot
                break
            if my_value_order > value_order:
                self.raw_slots.insert(i, raw_slot)
                break
        else:
            self.raw_slots.append(raw_slot)

    def set_raw_slot(self, value_order, value, description=None):
        r'''Update or create a raw_slot.

        The value_order may or may not match an existing value_order.  If it
        matches, this is an update; otherwise a create.

        Doesn't return anything.
        '''
        if value_order is None:
            raise AssertionError(
                    f'Updated multi-value slot {self.frame_id}.{self.name} '
                    f'value={value} has no "value_order"')

        for i, my_raw_slot in enumerate(self.raw_slots):
            my_value_order = my_raw_slot['value_order']
            if my_value_order == value_order:
                slot_id = self.frame.version_obj.update_slot(
                            my_raw_slot['slot_id'], value=value,
                            description=description)
                raw_slot['slot_id'] = slot_id
                raw_slot['value'] = value
                raw_slot['description'] = description
                break
            if my_value_order > value_order:
                raw_slot = self.frame.version_obj.create_slot(
                            self.frame.frame_id, self.name, value_order,
                            value, description)
                self.raw_slots.insert(i, raw_slot)
                break
        else:
            raw_slot = self.frame.version_obj.create_slot(
                        self.frame.frame_id, self.name, value_order,
                        value, description)
            self.raw_slots.append(raw_slot)

    def splice(self, i, splice_frame):
        r'''Splices new raw_slots into current raw_slots replacing i.
        
        Makes copies of each new raw_slot, then changes their value_orders.

        Returns the copied new raw_slots (in case you want to see their newly
        assigned value_orders).
        '''
        #print(f"splice({self.frame.frame_id}, {self.name})", i, splice_frame)
        new_raw_slots = []
        splice_raw_slot = self.raw_slots[i]
        #print("splice_raw_slot", splice_raw_slot)
        #assert splice_frame.get('isa') == 'splice', \
        #       f'Expected slot {splice_raw_slot["slot_id"]} to be a "splice"'
        name_to_splice = splice_raw_slot['name']  # Not upper!
        slot_list_to_splice = getattr(splice_frame, name_to_splice, None)
        #print("slot_list_to_splice", slot_list_to_splice)
        if slot_list_to_splice is not None:
            start = splice_raw_slot['value_order']
            if i == -1 or i == len(self.raw_slots) - 1:
                inc = 1.0
            else:
                end = self.raw_slots[i + 1]['value_order']
                inc = (end - start) / (len(slot_list_to_splice) + 1)
            for n, raw_value in enumerate(slot_list_to_splice.iter_raw_slots(),
                                          1):
                copied_value = raw_value.copy()
                #print("copied_value", copied_value)
                copied_value['value_order'] = start + n * inc
                if isinstance(copied_value['value'], frame):
                    for name in splice_frame.get_slot_names():
                        if name not in ('ISA', 'AKO', 'FRAME_NAME',
                                        name_to_splice.upper()):
                            slot_to_stuff = splice_frame.get_raw_slot(name)
                            copied_value['value'].insert_raw_slot(name,
                                                                  slot_to_stuff)
                #if not 'from_splice' in copied_value:
                #    copied_value['from_splice'] = []
                #copied_value['from_splice'].append(splice_raw_slot)
                new_raw_slots.append(copied_value)
        self.raw_slots[i:i+1] = new_raw_slots
        return new_raw_slots


def load_yaml(db, filename):
    from yaml import load
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
    with open(filename, 'r') as file:
        data = load(file, Loader=Loader)
    for objects in data:
        if 'users' in objects:
            load_users(db, objects)
        elif 'versions' in objects:
            load_versions(db, objects)
        elif 'frames' in objects:
            load_frames(db, objects)
        else:
            raise AssertionError(f"Unknown table {objects}")
    db.commit()

def load_users(db, objects):
    for user in objects['users']:
        print("loading user", user['name'])
        if 'email' not in user:
            user['email'] = None
        db.execute("""INSERT INTO User (login, password, name, email)
                      VALUES (:login, :password, :name, :email)""",
                   **user)

def load_versions(db, objects):
    user_name = objects['user']
    db.execute("SELECT user_id FROM user WHERE name = :user_name",
               user_name=user_name)
    user_id, = db.fetchone()
    for version in objects['versions']:
        name = version['name']
        print("loading version", name)
        db.execute("""
             INSERT INTO Version (name, name_upper, description,
                                  creation_user_id, creation_timestamp)
             VALUES (:name, :name_upper, :description, :creation_user_id,
                     datetime("now"))""",
             name=name, name_upper=name.upper(),
             description=version.get('description'), creation_user_id=user_id)
        version_id = db.lastrowid()
        for v in version.get('requires', ()):
            db.execute("""
                 INSERT INTO Version_requires
                   (version_id, required_version_id, creation_user_id,
                    creation_timestamp)
                 SELECT :version_id, version_id, :creation_user_id,
                        datetime("now")
                   FROM Version
                  WHERE name = :v""",
                 version_id=version_id, v=v, creation_user_id=user_id)


def load_frames(db, objects):
    user_name = objects['user']
    db.execute("SELECT user_id FROM user WHERE name = :user_name",
               user_name=user_name)
    user_id, = db.fetchone()
    version_obj = db_obj.at_versions(user_id, *objects['required_versions'])
    for frame in objects['frames']:
        print("loading frame", frame.get('frame_name') or frame.get('name'))
        version_obj.create_frame(frame)



if __name__ == "__main__":
    import sys
    import sqlite3

    def add_row_factory(conn):
        conn.row_factory = sqlite3.Row
    db_obj = db(sqlite3, "test.db", post_connect=add_row_factory)

    if False:
        print(load_yaml(db_obj, "frame_data.yaml"))
    else:
        db_obj.execute("""SELECT user_id FROM User WHERE name = 'bruce';""")
        user_id, = db_obj.fetchone()

        frame_label = int(sys.argv[1])
        version_obj = db_obj.at_versions(user_id, *sys.argv[2:])

        #print("version_ids", version_obj.version_ids)
        #print("required_versions", version_obj.required_versions)
        #print("required_map", version_obj.required_map)

        def print_slots(frame):
            print('slot_id', 'frame_id', 'name', 'value')
            for _, row in sorted(frame.items(), key=lambda item: item[0][1:]):
                if row['value_order'] is not None:
                    print(row['slot_id'], row['frame_id'],
                          f"{row['name']}[{row['value_order']}]:", row['value'])
                else:
                    print(row['slot_id'], row['frame_id'], f"{row['name']}:",
                          row['value'])

        #frame_id, raw_frame = version_obj.get_raw_frame(frame_label)
        #print_slots(raw_frame)
        #print_slots(version_obj.with_inherited_slots(frame_id, raw_frame))

        the_frame = version_obj.get_frame(frame_label)
        the_frame.dump()

        #print(version_obj.frame_ids_with_slots(isa='table', name='*'))
        #print(version_obj.frame_ids_with_slots(name='*'))

