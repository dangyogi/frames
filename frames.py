# frames.py

import re
from itertools import groupby, chain
from operator import itemgetter
from collections import defaultdict


class db:
    r'''Encapsulates the various database modules.

    Provides a unified interface to the various forms of sql parameter passing.

    This uses two forms of named sql params:
        - :name  -- for a single value
        - ::name -- for an iterable of values (used within parens in the SQL)
          - example: ... WHERE some_col IN (::values) ...
            with the sql_param: values=[1, 2, 3]

    This executes all sql statements on the same cursor.

    The only cursor method available is to treat the db as an iterator,
    which iterates on the cursor.
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

    def at_versions(self, *version_names):
        return version(self, *version_names)

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


class version:
    def __init__(self, db, *version_names):
        self.db = db
        self.version_names = version_names
        self.lookup_version_ids()
        self.required_versions, self.required_map = \
          self.get_all_required_versions()

    def lookup_version_ids(self):
        self.db.execute("""SELECT version_id, status FROM Version
                            WHERE name IN (::version_names)""",
                        version_names=self.version_names)
        self.version_ids = []
        self.frozen = True
        for row in self.db:
            self.version_ids.append(row[0])
            if row[1] == 'proposed':
                self.frozen = False

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

        Only does == matches.

        Returns a set of frame_ids.
        '''
        slot_names = frozenset(slots.keys())
        slot_names_with_ako = slot_names.union(['ako'])
        slots_found = self.select_slots_by_version(
                        "name IN (::slot_names_with_ako)",
                        slot_names_with_ako=slot_names_with_ako)

        # {base_id: {derived_id}}
        derived_map = defaultdict(set)
        for (frame_id, name, _), slot in slots_found.items():
            if name == 'ako':
                derived_map[slot['value']].add(frame_id)

        def frames_with_slot(slot_name, value):
            for (frame_id, name, value_order), slot in slots_found.items():
                if name == slot_name and value in (slot['value'], '*'):
                    yield frame_id
                    yield from spew_derived(frame_id, name, value_order)

        def spew_derived(frame_id, name, value_order):
            for d in derived_map[frame_id]:
                if (d, name, value_order) not in slots_found:
                    yield d
                    yield from spew_derived(d, name, value_order)

        found = [frozenset(frames_with_slot(slot_name, value))
                 for slot_name, value in slots.items()]
        return found[0].intersection(*found[1:])

    def get_raw_frame(self, frame_id):
        r'''Reads one frame from the database.
        
        Only includes the proper slots to use for this set of versions.

        Does not include inherited slots.
        
        Returns {(frame_id, name, value_order): slot}

        Where slot is dict with the following keys:
            - frame_id
            - slot_id
            - name
            - value_order
            - description
            - type
            - value
        '''
        return self.select_slots_by_version("frame_id = :frame_id",
                                            frame_id=frame_id)

    def select_slots_by_version(self, where_exp, **sql_params):
        r'''Figures slots matching where_exp/sql_params that are best match to
        my versions.

        Returns iterator generating selected slot rows.  This is the database
        cursor, so you must exhaust this before using the cursor for something
        else.

        Slots are generated ordered by frame_id, name, value_order.
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
                            ORDER BY frame_id, name, value_order;""",
                        slot_ids=matching_slot_ids)

        def get_value(row):
            values = [row[col]
                      for col in ('text_value', 'int_value', 'real_value',
                                  'boolean_value', 'date_value', 'time_value',
                                  'time_tz_value', 'timestamp_value',
                                  'timestamp_tz_value', 'interval_value')
                      if row[col] is not None]
            if not values:
                return None
            assert len(values) == 1, \
                   f"multiple type values set on frame {frame_id}!"
            return values[0]

        return {(row['frame_id'], row['name'], row['value_order']):
                dict(frame_id=row['frame_id'],
                     slot_id=row['slot_id'],
                     name=row['name'],
                     value_order=row['value_order'],
                     description=row['description'],
                     type=row['type'],
                     value=get_value(row))
                for row in self.db}

    def select_slot_ids_by_version(self, raw_slot_rows):
        r'''raw_slot_rows is (frame_id, name, value_order, slot_id, version_id)

        raw_slots must be sorted by frame_id, name, value_order, slot_id.

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
        ako = raw_frame.get((frame_id, 'ako', None))
        if ako and ako['type'] == 'frame':
            base_frame_id = ako['value']
            base_frame = self.get_raw_frame(base_frame_id)
            return derive(raw_frame,
                          self.with_inherited_slots(base_frame_id, base_frame))
        return raw_frame

    def get_frame(self, frame_id, format_slots=True):
        r'''Returns a frame object.

        Includes inherited slots.

        Reads in all sub-frames.
        '''
        raw_frame = self.get_raw_frame(frame_id)
        return frame(frame_id, self,
                     self.with_inherited_slots(frame_id, raw_frame),
                     format_slots=format_slots)


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
            if slot['type'] != 'delete':
                #print("taking", base_key, "from base")
                ans[base_key] = slot
    #print("from base", ans)
    for key, slot in derived.items():
        if slot['name'] != 'ako':
            #print("taking", key, "from derived")
            ans[key] = slot
    return ans


def expand_formats(frame):
    for slot in frame.values():
        if slot['type'] == 'format':
            slot['format'] = slot['value']  # save format
            slot['value'] = slot['format'].format(**slot)
            slot['type'] = 'string'


class frame:
    r'''Interface object for a frame.

    some_frame.slot_name -> value (may be a slot_list) # but not type='delete'
    some_frame.get_raw_slot(slot_name)  # see get_raw_frame for raw slots
                                        # returnes type='delete'
    some_frame.get_slot_names() -> iterable of slot_names (excluding deleted)
    '''
    def __init__(self, frame_id, version_obj, raw_frame, format_slots=True):
        self.frame_id = frame_id
        self.version_obj = version_obj
        self.raw_slots = {}  # {name: raw_slot|slot_list}
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
                self.raw_slots[name] = slot_list(self,
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

    def __getattr__(self, slot_name):
        slot = self.get_raw_slot(slot_name)
        if isinstance(slot, slot_list):
            return slot
        if slot['type'] == 'delete':
            raise AttributeError(f"{slot_name} deleted")
        return slot['value']

    def get_slot_names(self):
        return [key for key, slot in self.raw_slots.items()
                    if isinstance(slot, slot_list) or slot['type'] != 'delete']

    def get_raw_slot(self, slot_name):
        try:
            return self.raw_slots[slot_name]
        except KeyError:
            raise AttributeError(slot_name)

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
        if name in self.raw_slots and \
           isinstance(self.raw_slots[name], slot_list):
            # This may never get used...
            self.raw_slots[name].insert_raw_slot(raw_slot)
        else:
            self.raw_slots[name] = raw_slot

    def fill_frame(self):
        r'''Gets all frames referred to by "frame" type slots.

        Replaces the "value" (was the frame_id) with the entire frame.

        Also splices multi-valued slots where the sub-frame "isa" "splice".

        Does of all this in place.  Returns nothing.
        '''
        for name, slot in self.raw_slots.items():
            #print("fill_frame", name, slot)
            if isinstance(slot, slot_list):
                #print("got slot_list")
                i = 0
                while i < len(slot):
                    raw_slot = slot.get_raw_slot(i)
                    #print("index", i, raw_slot)
                    if raw_slot['type'] == 'frame':
                        sub_frame = \
                          self.version_obj.get_frame(raw_slot['value'], False)
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
            elif slot['type'] == 'frame':
                #print("not slot_list")
                sub_frame = self.version_obj.get_frame(slot['value'], False)
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
        context['self'] = self
        def format_slot(raw_slot):
            if isinstance(raw_slot, slot_list):
                for list_raw_slot in raw_slot.iter_raw_slots():
                    format_slot(list_raw_slot)
            else:
                if raw_slot['type'] == 'format':
                    try:
                        raw_slot['value'] = raw_slot['value'].format(**context)
                    except AttributeError:
                        # assume this frame is designed to only be used as ako
                        # where derived frame defines what's needed in the
                        # format.
                        pass
                elif isinstance(raw_slot['value'], frame):
                    raw_slot['value'].format_slots(context)
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
    def __init__(self, frame, raw_slots):
        self.frame = frame
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

    def splice(self, i, splice_frame):
        r'''Splices new raw_slots into current raw_slots replacing i.
        
        Makes copies of each new raw_slot, then changes their value_orders.

        Returns the copied new raw_slots (in case you want to see their newly
        assigned value_orders).
        '''
        new_raw_slots = []
        splice_raw_slot = self.raw_slots[i]
        #print("splice_raw_slot", splice_raw_slot)
        #assert splice_raw_slot.get('isa') == 'splice', \
        #       f'Expected slot {splice_raw_slot["slot_id"]} to be a "splice"'
        name_to_splice = splice_raw_slot['name']
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
                        if name not in ('isa', name_to_splice):
                            slot_to_stuff = splice_frame.get_raw_slot(name)
                            copied_value['value'].insert_raw_slot(name,
                                                                  slot_to_stuff)
                #if not 'from_splice' in copied_value:
                #    copied_value['from_splice'] = []
                #copied_value['from_splice'].append(splice_raw_slot)
                new_raw_slots.append(copied_value)
        self.raw_slots[i:i+1] = new_raw_slots
        return new_raw_slots



if __name__ == "__main__":
    import sys
    import sqlite3

    def add_row_factory(conn):
        conn.row_factory = sqlite3.Row
    db_obj = db(sqlite3, "test.db", post_connect=add_row_factory)

    frame_id = int(sys.argv[1])
    version_obj = db_obj.at_versions(*sys.argv[2:])

    #print("version_ids", version_obj.version_ids)
    #print("required_versions", version_obj.required_versions)
    #print("required_map", version_obj.required_map)

    def print_slots(frame):
        print('slot_id', 'frame_id', 'name', 'value_order', 'type', 'value')
        for _, row in sorted(frame.items(), key=lambda item: item[0][1:]):
            print(row['slot_id'], row['frame_id'], row['name'],
                  row['value_order'], row['type'], row['value'])

    #raw_frame = version_obj.get_raw_frame(frame_id)
    #print_slots(raw_frame)
    #print_slots(version_obj.with_inherited_slots(frame_id, raw_frame))

    the_frame = version_obj.get_frame(frame_id)
    the_frame.dump()

    #print(version_obj.frame_ids_with_slots(isa='table', name='*'))
    #print(version_obj.frame_ids_with_slots(name='*'))
