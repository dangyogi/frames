# attr_soup.py

from itertools import groupby, chain
from operator import itemgetter


def gen_template(l):
    return ', '.join('?' * len(l))


class version:
    def __init__(self, cursor, *version_names):
        self.cursor = cursor
        self.version_names = version_names
        self.lookup_version_ids()
        self.required_versions, self.required_map = \
          self.get_all_required_versions()

    def lookup_version_ids(self):
        self.cursor.execute(f"""
                        SELECT version_id, status FROM Version
                         WHERE name IN ({gen_template(self.version_names)})""",
                      self.version_names)
        self.version_ids = []
        self.frozen = True
        for row in self.cursor:
            self.version_ids.append(row[0])
            if row[1] == 'proposed':
                self.frozen = False

    def get_all_required_versions(self, seen=None, depth=0):
        r'''Figures out all of the required version info.

        Returns ({required_version_id}, {version_id: set(required_version_ids)})
        '''
        self.cursor.execute(f"""
                          WITH RECURSIVE req(ver_id, req_ver_id) AS (
                        SELECT version_id, required_version_id
                          FROM version_requires
                         WHERE version_id in ({gen_template(self.version_ids)})
                      UNION ALL
                        SELECT version_id, required_version_id
                          FROM version_requires
                               INNER JOIN req
                         WHERE version_id == req_ver_id
                      )
     
                      SELECT ver_id, req_ver_id FROM req
                       ORDER BY ver_id;""",
                    self.version_ids)
        required_map = {version_id: set(req_ver_id
                                        for ver_id, req_ver_id
                                         in required_versions)
                        for version_id, required_versions
                         in groupby(self.cursor, key=itemgetter(0))}
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

    def get_raw_frame(self, frame_id):
        r'''Reads one frame from the database.
        
        Only includes the proper slots to use for this set of versions.

        Does not include inherited slots.
        
        Returns {(name, value_order): slot}

        Where slot is dict with the following keys:
            - frame_id
            - slot_id
            - name
            - value_order
            - description
            - type
            - value
        '''
        #print("get_raw_frame", frame_id)
        self.cursor.execute(f"""SELECT name, value_order, slot_id, version_id
                                  FROM Slot
                                       INNER JOIN Slot_versions USING (slot_id)
                                 WHERE frame_id = ?
                                 ORDER BY name, value_order, slot_id;""",
                            [frame_id])
        matching_slot_ids = []
        for (name, value_order), slots in groupby(self.cursor,
                                                  key=itemgetter(0, 1)):
            matching_slots = []
            for slot_id, versions in groupby(slots, key=itemgetter(2)):
                version_ids = frozenset(v[3] for v in versions)
                if version_ids.issubset(self.required_versions):
                    matching_slots.append((slot_id, version_ids))

            #print("matching_slots", matching_slots)
            if len(matching_slots) == 1:
                matching_slot_ids.append(matching_slots[0][0])
            else:
                best_match = None
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

        self.cursor.execute(f"""
                        SELECT *
                          FROM Slot
                         WHERE slot_id IN ({gen_template(matching_slot_ids)})
                         ORDER BY name, value_order;""",
                    matching_slot_ids)

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

        return {(row['name'], row['value_order']):
                dict(frame_id=row['frame_id'],
                     slot_id=row['slot_id'],
                     name=row['name'],
                     value_order=row['value_order'],
                     description=row['description'],
                     type=row['type'],
                     value=get_value(row))
                for row in self.cursor}

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

    def with_inherited_slots(self, raw_frame):
        r'''Returns a new raw_frame that includes its inherited slots.

        Returns same structure as get_raw_frame.
        '''
        #print("with_inherited_slots", frame_id)
        ako = raw_frame.get(('ako', None))
        if ako and ako['type'] == 'frame':
            base_frame = self.get_raw_frame(ako['value'])
            return derive(raw_frame, self.with_inherited_slots(base_frame))
        new_frame = raw_frame.copy()
        for key, slot in raw_frame.items():
            if slot['type'] == 'delete':
                del new_frame[key]
        return new_frame

    def get_frame(self, frame_id):
        r'''Returns a frame object.

        Includes inherited slots.
        '''
        raw_frame = self.get_raw_frame(frame_id)
        return frame(frame_id, self, self.with_inherited_slots(raw_frame))


def derive(derived, base):
    #print("derive")
    ans = {}
    for base_key, slot in base.items():
        if base_key not in derived:
            if slot['type'] != 'delete':
                #print("taking", base_key, "from base")
                ans[base_key] = slot
    #print("from base", ans)
    for key, slot in derived.items():
        if slot['type'] != 'delete' and slot['name'] != 'ako':
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

    some_frame.slot_name -> value (may be a slot_list)
    some_frame.get_raw_slot(slot_name)
    some_frame.get_slot_names() -> iterable of slot_names
    '''
    def __init__(self, frame_id, version_obj, raw_frame):
        #print("frame", frame_id)
        self.frame_id = frame_id
        self.version_obj = version_obj
        self.raw_slots = {}  # {name: raw_slot|slot_list}
        for name, slots_by_name in groupby(sorted(raw_frame.items()),
                                           key=lambda item: item[0][0]):
            first_slot = next(slots_by_name)[1]
            if first_slot['value_order'] is None:
                self.raw_slots[name] = first_slot
                assert next(slots_by_name, 'empty') == 'empty', \
                       "Got value_order of None in multi-valued slot: " \
                       f"slot_id {first_slot[slot_id]}"
            else:
                #print("frame got multi-value", name)
                self.raw_slots[name] = slot_list(self,
                                             chain([first_slot],
                                                   (item for key, item
                                                    in slots_by_name)))
    
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
        return slot['value']

    def get_slot_names(self):
        return self.raw_slots.keys()

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
                          self.version_obj.get_frame(raw_slot['value'])
                        sub_frame.fill_frame()
                        #print("sub_frame ", end='')
                        #sub_frame.print()
                        if getattr(sub_frame, 'isa') == 'splice':
                            #print("sub_frame is splice")
                            new_slots = slot.splice(i, sub_frame)
                            i += len(new_slots)
                        else:
                            raw_slot['value'] = sub_frame
                            #print("sub_frame not splice", raw_slot)
                            i += 1
                #print("final slot_list", slot)
            elif slot['type'] == 'frame':
                #print("not slot_list")
                sub_frame = self.version_obj.get_frame(slot['value'])
                sub_frame.fill_frame()
                if getattr(sub_frame, 'isa') == 'splice':
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
                    raw_slot['value'] = raw_slot['value'].format(**context)
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

        Stores the deleted raw_slot on each new value's "from_splice"
        attribute.

        Returns the copied new raw_slots (in case you want to see their newly
        assigned value_orders).
        '''
        new_raw_slots = []
        splice_raw_slot = self.raw_slots[i]
        #print("splice_raw_slot", splice_raw_slot)
        #assert splice_raw_slot.get('isa') == 'splice', \
        #       f'Expected slot {splice_raw_slot["slot_id"]} to be a "splice"'
        name_to_splice = splice_raw_slot['name']
        slot_list_to_splice = getattr(splice_frame, name_to_splice)
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

    conn = sqlite3.connect("test.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    version_obj = version(cursor, *sys.argv[2:])

    #print("version_ids", version_obj.version_ids)
    #print("required_versions", version_obj.required_versions)
    #print("required_map", version_obj.required_map)

    def print_slots(frame):
        print('slot_id', 'frame_id', 'name', 'value_order', 'type', 'value')
        for _, row in sorted(frame.items()):
            print(row['slot_id'], row['frame_id'], row['name'],
                  row['value_order'], row['type'], row['value'])

    #raw_frame = version_obj.get_raw_frame(int(sys.argv[1]))
    #print_slots(raw_frame)
    #print_slots(version_obj.with_inherited_slots(raw_frame))

    the_frame = version_obj.get_frame(int(sys.argv[1]))
    the_frame.fill_frame()
    the_frame.format_slots()
    the_frame.dump()

