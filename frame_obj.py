# frame_obj.py

from itertools import groupby, chain
from operator import itemgetter
from collections import defaultdict, deque, ChainMap

from db import connection


def asbool(x):
    r'''Converts slot value (a python str) to a python bool.
    '''
    if x.lower() == 'true':
        return True
    if x.lower() == 'false':
        return False
    raise ValueError(f"{x!r} is not a legal boolean value")


def islist(x):
    r'''True iff `x` is some kind of list.
    '''
    return isinstance(x, (slot_list, dynamic_slot_list, list, tuple))


def aslist(x):
    r'''Makes sure that `x` is some kind of list.

    If not, creates a list with `x` as the sole element.
    '''
    if islist(x):
        return x
    return [x]


class context:
    r'''Only used for context for format values.

    Makes keys case insensitive.
    '''
    def __init__(self, parent):
        self.parent = parent

    def __getitem__(self, name):
        parent = self.parent
        if name.lower() == 'frame':
            assert parent is not None
            #print(f"context[{name}] -> {parent}")
            return parent
        while parent is not None:
            #print(f"context[{name}] checking {parent}")
            try:
                class_name = parent.get_slot('class_name')
            except AttributeError:
                pass
            else:
                #print(f"context[{name}] got {class_name!r}")
                if class_name.lower() == name.lower():
                    #print(f"context[{name}] -> {parent}")
                    return parent
            parent = getattr(parent, 'parent', None)
        raise KeyError(name)


class version:
    r'''This provides the high-level access to a frames database.

    It pulls together the db connection used to access the database, the
    user_id for the frames user making requests on this database, and a
    sequence of version_names that collectively identify which specific
    version of the database to present.

    Methods:

        - get_frame(frame_label)
          The frame_label may be a frame id (as int or str), or frame_name.
          Returns a frame object with all inherited slots.

        - get_raw_frame(frame_label)
          The frame_label may be a frame id (as int or str), or frame_name.
          Returns a single frame (without inherited slots) in a raw format.

        - frame_ids_with_slots(**slots)
          Returns a set of frame_ids.

        - delete_slot(slot_id)

        - create_list(frame, name, values)

        - update_slot(slot_id, value, slot_list_order=None, description=None)
          Returns slot_id

        - create_slot(frame_id, name, value, slot_list_order=None, description=None)
          Returns a raw_slot (see get_raw_frame for what a "raw_slot" is).

        - load_frame(slots)
          `slots` is {name: value}.

    Updates to the frame are done directly on the frame object.
    '''
    def __init__(self, db_conn, user_id, *version_names):
        self.db_conn = db_conn
        self.user_id = user_id
        self.version_names = version_names
        self.lookup_version_ids()  # sets self.version_ids to set ids for names

        # self.required_versions is the set of all versions (recursively)
        #                           required by self
        # self.required_map      is {version_id: set of required_version_ids}
        #                           for all version_ids in required_versions
        self.required_versions, self.required_map = \
          self.get_all_required_versions()
        #print("version", self.required_versions, self.required_map)

        # {frame_name.upper(): frame_id}
        self.frame_names = {
          value.upper(): frame_id
          for frame_id, slot_id, value
           in self.select_slot_ids_by_version("name = 'frame_name'")}
        #print("frame_names", self.frame_names)
        #print()

        # [(parent_id, child_id)]
        slots = [(frame_id, (int(value[1:])
                             if value[1:].isdigit()
                             else self.frame_names[value[1:].upper()]))
                 for frame_id, slot_id, value
                  in self.select_slot_ids_by_version(
                       "value LIKE '$%' "
                       "AND name != 'ako' AND name != 'isa'")]

        # {frame_id: set(parent_frame_id)}
        self.parent_links = {
          child_id: {parent_id for parent_id, _ in parents}
          for child_id, parents
           in groupby(sorted(slots, key=itemgetter(1)), key=itemgetter(1))}
        #print("parent_links", self.parent_links)
        #print()

        self.frame_ids = {}    # {frame_id: frame}

    def lookup_version_ids(self):
        self.db_conn.execute("""SELECT version_id, name, status
                                  FROM Version
                                 WHERE name IN (::version_names)""",
                             version_names=self.version_names)
        version_ids = []
        self.frozen = True
        names = {name.upper(): name for name in self.version_names}
        for row in self.db_conn:
            version_ids.append(row['version_id'])
            if row['status'] == 'proposed':
                self.frozen = False
            del names[row['name'].upper()]
        if names:
            raise AssertionError(
                    f"Version names not found: {sorted(names.values())}")
        self.version_ids = frozenset(version_ids)

    def get_all_required_versions(self, seen=None, depth=0):
        r'''Figures out all of the required version info.

        Returns ({required_version_id}, {version_id: set(required_version_ids)})
        '''
        self.db_conn.execute("""
          WITH RECURSIVE req(ver_id, req_ver_id)
            AS (  SELECT version_id, required_version_id
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
                                        for _, req_ver_id
                                         in required_versions)
                        for version_id, required_versions
                         in groupby(self.db_conn, key=itemgetter(0))}
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
        slot_names_with_ako = slot_names.union(['AKO', 'ISA'])
        raw_frames = self.select_slots_by_version(
                       "name IN (::slot_names_with_ako)",
                       dict(slot_names_with_ako=slot_names_with_ako))

        # {base_id: {derived_id}}
        derived_map = defaultdict(set)
        for (frame_id, name, slot_list_order), raw_slot in raw_frames.items():
            if name in ('AKO', 'ISA'):
                # FIX: what if raw_slot is a slot_list?
                derived_map[raw_slot['value']].add(frame_id)

        def frames_with_slot(slot_name, value):
            r'''Slot_name is passed in uppercase.
            '''
            if isinstance(value, frame):
                value = frame.frame_id
            if isinstance(value, str):
                value = value.upper()
            for (frame_id, name, slot_list_order), slot in raw_frames.items():
                if name == slot_name and \
                   value in ((slot['value'].upper()
                              if isinstance(slot['value'], str)
                              else slot['value']),
                             '*'):
                    yield frame_id
                    if slot_name != 'FRAME_NAME':
                        yield from spew_derived(frame_id, name, slot_list_order)

        def spew_derived(frame_id, name, slot_list_order):
            for d in derived_map[frame_id]:
                if (d, name, slot_list_order) not in slots_found:
                    yield d
                    yield from spew_derived(d, name, slot_list_order)

        found = [frozenset(frames_with_slot(slot_name.upper(), value))
                 for slot_name, value in slots.items()]
        return found[0].intersection(*found[1:])

    def get_raw_frame(self, frame_label):
        r'''Reads one frame from the database.
        
        frame_label can be either a frame_id (either int or str),
        or a frame_name.

        Only includes the proper slots to use for this set of versions.

        Does not include inherited slots.
        
        Returns frame_id, {(frame_id, name.upper(), slot_list_order): slot}

        Where slot is dict with the following keys:
            - frame_id
            - slot_id
            - name  # value not upper cased
            - slot_list_order
            - description
            - value # may be "<DELETED>"
        '''
        if isinstance(frame_label, int) or \
           isinstance(frame_label, str) and frame_label.isdigit():
            frame_id = int(frame_label)
        else:
            frame_id = self.frame_names[frame_label.upper()]
        return (frame_id,
                self.select_slots_by_version("frame_id = :frame_id",
                                             frame_id=frame_id))

    def select_slots_by_version(self, where_exp, **sql_params):
        r'''Figures slots matching where_exp/sql_params that are best match to
        my versions.

        Returns {(frame_id, slot_name, slot_list_order): raw_slot}.
        '''
        matching_slot_ids = [slot_id
                             for frame_id, slot_id, value 
                              in self.select_slot_ids_by_version(where_exp,
                                                                 sql_params)]

        self.db_conn.execute("""SELECT *
                                  FROM Slot
                                 WHERE slot_id IN (::slot_ids);""",
                             slot_ids=matching_slot_ids)

        return {(row['frame_id'], row['name'].upper(), row['slot_list_order']):
                dict(frame_id=row['frame_id'],
                     slot_id=row['slot_id'],
                     name=row['name'],
                     slot_list_order=row['slot_list_order'],
                     description=row['description'],
                     value=row['value'])
                for row in self.db_conn}

    def select_slot_ids_by_version(self, where_exp, sql_params={}):
        r'''Finds matching slots that are best match to my versions.

        Finds slots matching where_exp/sql_params.

        Returns [(frame_id, slot_id, value)]
        '''
        self.db_conn.execute(f"""
          WITH desired_slots(frame_id, name, slot_list_order, slot_id, value)
            AS (SELECT frame_id, name, slot_list_order, slot_id, value
                  FROM Slot
                 WHERE {where_exp})

          SELECT desired_slots.*, 1, version_id
            FROM desired_slots
                 INNER JOIN Slot_versions USING (slot_id)

          UNION ALL

          -- Undesired slots that might be a better version match than the
          -- desired ones (and, hence, hide the desired value)!
          SELECT frame_id, name, slot_list_order, s.slot_id, NULL, 0, version_id
            FROM desired_slots ds
                 INNER JOIN Slot s USING (frame_id, name, slot_list_order)
                 INNER JOIN Slot_versions v ON v.slot_id = s.slot_id
           WHERE ds.slot_id != s.slot_id;""",
          **sql_params)

        return self.select_best_matches(self.db_conn)

    def select_best_matches(self, raw_slot_rows):
        r'''Selects desired slots that are the best match to my versions.

        raw_slot_rows is (frame_id, name, slot_list_order, slot_id, value, desired,
        version_id)

        Returns [(frame_id, slot_id, value)]
        '''
        sorted_slots = sorted(raw_slot_rows,
                              key=lambda row: (row[0], row[1].upper(),
                                               row[2], row[3]))

        matching_slot_ids = []

        # for each slot name:
        for (frame_id, name, slot_list_order), slots \
         in groupby(sorted_slots,
                    key=lambda row: (row[0], row[1].upper(), row[2])):

            # [(slot_id, value, desired, version_ids_frozenset)]
            matching_slots = []

            # Gather slot_ids that have all of my required_versions.
            for (slot_id, value, desired), versions \
             in groupby(slots, key=itemgetter(3, 4, 5)):
                version_ids = frozenset(v[6] for v in versions)
                if version_ids.issubset(self.required_versions):
                    matching_slots.append((slot_id, value,
                                           desired, version_ids))
            #print("matching_slots", matching_slots)

            # Find best match
            if len(matching_slots) == 1:
                # Only one slot_id found, it's the best match!
                if matching_slots[0][2]: # desired
                    matching_slot_ids.append((frame_id, matching_slots[0][0],
                                              matching_slots[0][1]))
            elif matching_slots:
                best_match = None  # (slot_id, versions)

                # Try each slot to see which one is the best match.
                for slot_id, value, desired, versions in matching_slots:
                    #print("checking", slot_id, versions)

                    # Look for better match in other slots.
                    for slot_id2, _, _, versions2 in matching_slots:
                        if slot_id != slot_id2 and \
                           not self.better_fit(slot_id, versions,
                                               slot_id2, versions2):
                            # nope, slot_id is not the best match!
                            break
                    else:
                        # No better fit, slot_id is the best one!
                        if best_match is not None:
                            # Conflict!
                            # How could this happen??
                            raise AssertionError(
                                    "Impossible slot version conflict between "
                                    f"{matching_slots}")
                        else:
                            best_match = (slot_id, value, desired, versions)
                if best_match is not None:
                    # Best match found!
                    if best_match[2]: # desired
                        matching_slot_ids.append((frame_id, best_match[0],
                                                  best_match[1]))
                else:
                    # None of the versions stands out as being better than all
                    # of the rest...
                    matches = ', '.join('{}{}'.format(s, list(v))
                                        for s, _, _, v in matching_slots)
                    raise AssertionError(
                            f"Slot version conflict between {matches}")
        return matching_slot_ids

    def better_fit(self, slot_id, versions, other_slot_id, other_versions):
        #print("better_fit", versions, other_versions)
        if len(other_versions) > len(versions):
            #print("better_fit -> False, len(other_versions) > len(versions)")
            return False
        num_better = 0
        num_matches = 0
        for v in versions:
            for other_v in other_versions:
                #print("checking", v, "against", other_v)
                if v == other_v:
                    num_matches += 1
                elif v in self.required_map and other_v in self.required_map[v]:
                    # v is better than other_v
                    num_better += 1
                elif other_v in self.required_map and \
                     v in self.required_map[other_v]:
                    # other_v is better than v
                    return False
        #print("better_fit: num_better", num_better, "num_matches", num_matches)
        if num_better + num_matches < len(other_versions):
            # There are some disjoint versions between the two sets of versions
            return False
        if num_matches == len(other_versions) == len(versions):
            # The sets are the identical!
            # FIX: Should this be an exception because these two slots will
            #      always fail each other?
            return False
        ans = num_better or len(versions) > len(other_versions)
        #print("better_fit ->", ans, "final")
        return ans

    def get_frame(self, frame_label):
        r'''Returns a frame object.

        Does not include inherited slots.
        '''
        if isinstance(frame_label, int) or frame_label.isdigit():
            frame_id = int(frame_label)
        else:
            frame_id = self.frame_names[frame_label.upper()]
        if frame_id in self.frame_ids:
            return self.frame_ids[frame_id]
        return self.read_frame(frame_id)

    def read_frame(self, frame_id):
        r'''Reads a frame from the database.
        '''
        frame_id, raw_frame = self.get_raw_frame(frame_id)
        new_frame = frame.from_raw_frame(frame_id, self, raw_frame)
        self.frame_ids[frame_id] = new_frame
        return new_frame

    def lookup(self, frame_id, name):
        r'''Does a lookup for a frame with frame.class_name == `name`.

        This is called by the context.__getitem__ method, for use with format
        values.

        `name` must be lowercase.

        If the `name` is not found in frame_id, then that frame's parents are
        searched in a breadth-first order.

        Raises KeyError if `name` not found.
        '''
        frame = self.frame_ids[frame_id]
        if name == 'frame':
            return frame
        class_name = getattr(frame, 'class_name', None)
        if class_name is not None and class_name.lower() == name:
            return frame

        # breadth-first search...
        parents = deque(self.parent_links.get(frame_id, ()))
        while parents:
            try:
                return self.lookup(parents.popleft(), name)
            except KeyError:
                parents.extend(self.parent_links.get(frame_id, ()))
        raise KeyError(f"{name} in {frame.frame_label}")

    def delete_slot(self, slot_id):
        r'''Marks slot_id as deleted.

        Doesn't return anything.
        '''
        # FIX: Do I need to check the slot's versions before doing this
        #      (vs. creating a new slot)?
        self.db_conn.execute("""UPDATE Slot
                                   SET value = '<DELETED>'
                                 WHERE slot_id = :slot_id""",
                             slot_id=slot_id)

    def create_list(self, frame, name, values):
        r'''Creates a new set of slots, numbering slot_list_order from 1000 by 1.

        Returns a slot_list of the newly created values.
        '''
        raw_slots = []
        for slot_list_order, value in enumerate(values, 1000):
            raw_slots.append(self.create_slot(frame.frame_id, name, value,
                                              slot_list_order))
        return slot_list(frame, name, raw_slots)

    def update_slot(self, slot_id, value, slot_list_order=None, description=None):
        r'''Returns slot_id (may have had to create a new one)
        '''
        if self.frozen:
            raise AssertionError("Can not make changes to frozen versions")

        self.db_conn.execute("""SELECT version_id FROM Slot_versions
                                 WHERE slot_id = :slot_id""",
                             slot_id=slot_id)
        slot_versions = frozenset(row[0] for row in self.db_conn)
        if slot_versions == self.version_ids:
            # Current slot_id is for this version!  Update slot in place...
            if isinstance(value, frame):
                db_value = f"${value.frame_label}"
            else:
                db_value = value
            self.db_conn.execute("""
              UPDATE Slot
                 SET value = :value, slot_list_order = :slot_list_order
                     description = :description,
                     updated_user_id = :user_id,
                     updated_timestamp = datetime("now"),
               WHERE slot_id = :slot_id""",
              value=value, slot_list_order=slot_list_order, description=description,
              slot_id=slot_id, user_id=self.user_id)
            return slot_id

        # Otherwise, create new slot for this version...
        self.db_conn.execute("""SELECT frame_id, name
                                  FROM Slot
                                 WHERE slot_id = :slot_id""",
                             slot_id=slot_id)
        frame_id, name = self.db_conn.fetchone()
        raw_slot = self.create_slot(frame_id, name, slot_list_order, value,
                                    description)
        return raw_slot['slot_id']

    def create_slot(self, frame_id, name, value, slot_list_order=None,
                    description=None):
        r'''Returns a raw_slot (see get_raw_frame for what a "raw_slot" is).
        '''
        if self.frozen:
            raise AssertionError("Can not make changes to frozen versions")
        if isinstance(value, frame):
            db_value = f"${value.frame_label}"
        else:
            db_value = str(value)
        # Insert the new slot row
        self.db_conn.execute("""
          INSERT INTO Slot (frame_id, name, slot_list_order, value, description,
                            creation_user_id, creation_timestamp)
          VALUES (:frame_id, :name, :slot_list_order, :value, :description,
                  :creation_user_id, datetime("now"));""",
          frame_id=frame_id, name=name, slot_list_order=slot_list_order, value=db_value,
          description=description, creation_user_id=self.user_id)
        slot_id = self.db_conn.lastrowid

        # Assign version_ids to new slot
        for version_id in self.version_ids:
            self.db_conn.execute("""
              INSERT INTO Slot_versions (slot_id, version_id,
                                         creation_user_id, creation_timestamp)
              VALUES (:slot_id, :version_id,
                      :creation_user_id, datetime("now"));
              """,
              slot_id=slot_id, version_id=version_id,
              creation_user_id=self.user_id)

        return dict(frame_id=frame_id,
                    slot_id=slot_id,
                    name=name,
                    slot_list_order=slot_list_order,
                    description=description,
                    value=value)

    def load_frame(self, slots, last_frame_id=None):
        r'''Creates a new frame with the slots specified.

        `slots` is {name: value}.

        The slot_list_order of any list/tuple values are assigned starting at 1000.
        `slot_list_order` is always incremented by 1.

        Only called by load_yaml -> load_frames.

        Returns frame_id, frame_label ("$<frame_id>" or "$<frame_name>") for
        the new frame.
        '''

        # Figure out frame_id for new frame...
        self.db_conn.execute("""SELECT frame_id FROM Slot
                                 ORDER BY frame_id DESC
                                 LIMIT 1""")
        rows = self.db_conn.fetchall()
        if rows:
            assert len(rows) == 1
            if last_frame_id is None:
                frame_id = rows[0][0] + 1
            else:
                frame_id = max(rows[0][0], last_frame_id) + 1
        else:
            frame_id = 1

        last_frame_id = frame_id

        def unwrap_value_info(slot_name, value, version_obj=self,
                              current_index=None, slot_list_order_offset=None):
            r'''Unwraps all nested dict objects (if any).

            These could be "value_info" objects, or nested frames.
            
            A "value_info" object may stand in for a single value.  It will
            always have a "value" key.  (Thus, nested frames are not allowed
            to have a "value" slot)...

            This is a dict with the following keys (all but `value` are
            optional):
                - value             -- the value it's standing in for (required)
                - user_id           -- the user_id to store in creation_user_id
                - required_versions -- list of version names for slot_versions
                - slot_list_order       -- the offset to apply to slot_list_order
                                       within a list
                - slot_name         -- actual slot name
                                       (overrides `slot_name` param, allows
                                        multiple values for different versions
                                        stored under different (unique) keys)
                - description       -- defaults to None

            Inner value_info objects override outer ones.  FIX: Does this make
            sense?

            Returns slot_name, version_obj, slot_list_order_offset, value,
                    description
            '''
            nonlocal last_frame_id
            new_offset = None
            description = None
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
                    elif key_upper == 'SLOT_NAME':
                        slot_name = info
                    elif key_upper == 'DESCRIPTION':
                        description = info
                    elif key_upper == 'VALUE':
                        value = info
                    else:
                        raise AssertionError(
                                f"Unknown key, {key}, on slot {slot_name}")
                if user_id is not None or required_versions is not None:
                    version_obj = self.db_conn.at_versions(
                                    user_id or self.user_id,
                                    *(required_versions
                                        if required_versions is not None
                                        else self.version_names))
            if current_index is None and new_offset is not None:
                raise AssertionError(
                        "slot_list_order not allowed on single-valued "
                        f"slot {slot_name}")
            if isinstance(value, dict):   # nested frame
                last_frame_id, value = version_obj.load_frame(value,
                                                              last_frame_id)
            if new_offset is not None:
                return (slot_name, version_obj, new_offset - i, value,
                        description)
            return (slot_name, version_obj, slot_list_order_offset, value,
                    description)

        frame_label = f"${frame_id}"
        for name, value in slots.items():
            #print("create_frame", name, value)
            slot_name, version_obj, _, value, description = \
              unwrap_value_info(name, value)
            if not islist(value):
                if slot_name.upper() == 'FRAME_NAME':
                    frame_label = f"${value}"
                version_obj.create_slot(frame_id, slot_name, value,
                                        description=description)
            else:
                if slot_name.upper() in ("NAME", "AKO", "ISA", "FRAME_NAME",
                                         "CLASS_NAME", "SPLICE"):
                    raise AssertionError(
                            f"{slot_name} slot not allowed to have "
                            "multiple values")
                slot_list_order_offset = 1000
                for i, v in enumerate(value):
                    new_name, this_version_obj, slot_list_order_offset, v, \
                    description = \
                      unwrap_value_info(slot_name, v, version_obj, i,
                                        slot_list_order_offset)
                    if new_name != slot_name:
                        raise AssertionError(
                                "Not allowed to change slot name in "
                                f"multi-valued slot {name}")
                    this_version_obj.create_slot(frame_id, slot_name,
                                                 v, i + slot_list_order_offset,
                                                 description)
        return frame_id, frame_label


class frame:
    r'''Interface object for a frame.

    The inheritance is done dynamically when the slots are accessed.

    some_frame.slot_name -> value (may be a slot_list)
    some_frame.get_raw_slot(slot_name)  # Frame_slot row or slot_list
    some_frame.get_slot_names() -> iterable of slot_names
    '''
    def __init__(self, frame_id, version_obj, raw_slots,
                 parent=None, splices=()):
        self.frame_id = frame_id
        self.version_obj = version_obj

        # raw_slots is {slot_name.lower(): Frame_slot row or slot_list}
        # raw_slots is shared by dynamic frames, and they see all changes.
        self.raw_slots = raw_slots

        self.parent = parent
        self.splices = splices

    @classmethod
    def from_raw_data(cls, version_obj, frame_id, raw_data):
        r'''Creates new frame from a list of Frame_slot rows.

        The raw_data rows must be sorted by 'name', 'slot_list_order'.
        '''
        raw_slots = {}
        new_frame = cls(frame_id, version_obj, raw_slots)

        def make_value(slots_by_name):
            first_slot = next(slots_by_name)
            if first_slot['slot_list_order'] is None:
                next_slot = next(slots_by_name, None)
                if next_slot is not None:
                    raise AssertionError(
                            "Got slot_list_order of None in multi-valued slot: "
                            f"slot_id {first_slot['slot_id']}, "
                            f"next_slot {next_slot['slot_id']}")
                return first_slot
            return slot_list(new_frame, name, chain([first_slot],
                                                    slots_by_name))
        for name, slots_by_name in groupby(raw_data, key=itemgetter('name')):
            raw_slots[name.lower()] = make_value(slots_by_name)
        return new_frame

    @property
    def frame_label(self):
        return self.version_obj.lookup_frame_name(self.frame_id) \
            or self.frame_id

    def __repr__(self):
        #if hasattr(self, 'name'):
        #    if hasattr(self, 'class_name'):
        #        return f"<{self.__class__.__name__}({self.frame_label}): " \
        #               f"{self.class_name}({self.name})>"
        #    return f"<{self.__class__.__name__}({self.frame_label}): " \
        #           f"{self.name}>"
        if hasattr(self, 'class_name'):
            return f"<{self.__class__.__name__}({self.frame_label}): " \
                   f"{self.class_name}>"
        return f"<{self.__class__.__name__}({self.frame_label})>"

    def print(self):
        for name in sorted(self.get_slot_names()):
            print(name, getattr(self, name))

    def dump(self, indent=0):
        for name in sorted(self.get_slot_names()):
            value = self.get_slot(name, ignore_format_errors=True)
            print(' ' * indent, end='')
            if isinstance(value, frame) and name.lower() not in ('ako', 'isa'):
                print(f"{name}:")
                value.dump(indent + 2)
            elif isinstance(value, dynamic_slot_list):
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
                print(f"{name}:", repr(value))

    def is_frozen(self):
        return self.version_obj.is_frozen()

    def add_context(self, parent, splices):
        assert self.parent is None
        return frame(self.frame_id, self.version_obj, self.raw_slots,
                     parent, splices)

    def get_slot_names(self, seen_isa=False, seen_ako=False):
        r'''Returns set of slot_names.

        Includes inherited slots.  Excludes deleted slots.

        The returned names have been lowercased.
        '''
        # Get inherited 'AKO' slots:
        if 'ako' in self.raw_slots:
            ans = self.cook_raw_slot(self.get_raw_slot('ako')) \
                      .get_slot_names(seen_isa=seen_isa, seen_ako=True)
        else:
            ans = set()

        # Get inherited 'ISA' slots: (these override inherited 'AKO' slots)
        if not seen_isa and 'isa' in self.raw_slots:
            ans.update(self.cook_raw_slot(self.get_raw_slot('isa'))
                           .get_slot_names(seen_isa=True, seen_ako=seen_ako))

        # Add my slots: (these override inherited slots)
        for slot_name, slot in self.raw_slots.items():
            if not isinstance(slot, slot_list) and \
               slot['value'].upper() == '<DELETED>':
                ans.discard(slot_name)
            else:
                # Don't include 'FRAME_NAME' in inherited slots!
                skip1 = slot_name == 'frame_name' and (seen_isa or seen_ako)
                skip2 = slot_name == 'ako' and seen_isa
                if not skip1 and not skip2:
                    ans.add(slot_name)
        # Add spliced-in slots: (these override everything else!)
        for slot_list_name, frame in self.splices:
            for name, raw_slot in self.raw_slots.items():
                if name not in ('frame_name', 'class_name', 'isa', 'ako',
                                'splice', slot_list_name):
                    if not isinstance(raw_slot, slot_list) and \
                       raw_slot['value'].upper() == '<DELETED>':
                        ans.discard(name)
                    else:
                        ans.add(name)
        return ans

    def __getattr__(self, slot_name):
        return self.get_slot(slot_name)

    def get_slot(self, slot_name, ignore_format_errors=False):
        r'''`slot_name` can be any case (upper/lower).

        Raises AttributeError if not found.
        '''
        return self.cook_raw_slot(self.get_raw_slot_inherited(slot_name),
                                  ignore_format_errors=ignore_format_errors)

    def get_raw_slot_inherited(self, slot_name, try_isa=True):
        r'''Returns raw_slot, checking for inherited slots if necessary.

        Raises AttributeError if not found.
        '''
        #print(f"{self.frame_id}.get_raw_slot_inherited({slot_name}, "
	#      f"{try_isa})")

        try:
            # Check my slots:
            raw_slot = self.get_raw_slot(slot_name, deleted_is_error=False)
        except AttributeError:
            if slot_name.lower() != 'frame_name' and \
               (slot_name.lower() != 'ako' or try_isa):

                # Check inherited 'AKO' slots
                if 'ako' in self.raw_slots:
                    ako = self.cook_raw_slot(self.get_raw_slot('ako'),
                                             format_ok=False)
                    try:
                        return ako.get_raw_slot_inherited(slot_name, try_isa)
                    except AttributeError:
                        pass

                # Check inherited 'ISA' slots
                if try_isa and 'isa' in self.raw_slots:
                    isa = self.cook_raw_slot(self.get_raw_slot('isa'),
                                             format_ok=False)
                    try:
                        return isa.get_raw_slot_inherited(slot_name,
                                                          try_isa=False)
                    except AttributeError:
                        pass
            raise
        if not isinstance(raw_slot, slot_list) and \
           raw_slot['value'].upper() == '<DELETED>':
            raise AttributeError(f"{self.frame_label}.{slot_name} deleted")
        return raw_slot

    def get_inherited_values(self, slot_name, try_isa=True):
        r'''Get inherited values for slot_list.

        Returns {slot_list_order: raw_slot}.

        Does not include raw_slots in this frame itself.
        '''
        ans = {}
        def fetch(slot, try_isa):
            r'''Fetch elements inherited through `slot` link ('ISA', or 'AKO').

            `slot` must be lowercase.
            '''
            #print("fetch", slot, try_isa, "len(ans)", ans)
            if slot in self.raw_slots:
                daddy = self.cook_raw_slot(self.get_raw_slot(slot),
                                           format_ok=False)
                #print("fetch got slot, daddy", daddy.frame_label)
                ans.update(daddy.get_inherited_values(slot_name, try_isa))
                #print("fetch after daddy.get_inherited_values, len(ans)",
                #      len(ans))
                try:
                    daddy_list = daddy.get_raw_slot(slot_name,
                                                    deleted_is_error=False)
                except AttributeError:
                    #print("fetch: daddy does not have slot", slot_name)
                    pass
                else:
                    #print("fetch: daddy has slot", slot_name, daddy_list)
                    if not isinstance(daddy_list, slot_list):
                        ans.clear()  # deletes all prior inherited values...
                    else:
                        for daddy_slot in daddy_list.iter_raw_slots():
                            if daddy_slot['value'].upper() == '<DELETED>':
                                if daddy_slot['slot_list_order'] in ans:
                                    del ans[daddy_slot['slot_list_order']]
                            else:
                                ans[daddy_slot['slot_list_order']] = daddy_slot
        if try_isa:
            fetch('ISA', False)
        fetch('AKO', True)
        return ans

    def get_raw_slot(self, slot_name, deleted_is_error=True):
        r'''`slot_name` can be any case (upper/lower).

        Does not use inheritance to find `slot_name`.

        Does use spliced-in slots to find `slot_name`.

        Raises AttributeError if not found.
        '''
        #print(f"{self.frame_id}.get_raw_slot({slot_name})")

        # Check spliced-in slots:
        if slot_name.lower() not in ('frame_name', 'class_name', 'isa', 'ako',
                                     'splice'):
            for slot_list_name, frame in self.splices:
                if slot_name.lower() != slot_list_name.lower():
                    try:
                        return frame.get_my_raw_slot(slot_name)
                    except AttributeError:
                        pass

        # Check my slots:
        return self.get_my_raw_slot(slot_name, deleted_is_error)

    def get_my_raw_slot(self, slot_name, deleted_is_error=True):
        r'''`slot_name` can be any case (upper/lower).

        Does not use inheritance to find `slot_name`.

        Does not use spliced-in slots to find `slot_name`.

        Raises AttributeError if not found.
        '''
        # Check my slots:
        try:
            ans = self.raw_slots[slot_name.lower()]
        except KeyError:
            pass
        else:
            if not deleted_is_error or isinstance(ans, slot_list) \
               or ans['value'].upper() != '<DELETED>':
                return ans
        if slot_name.lower() != 'frame_name':
            raise AttributeError(f"{self.frame_label}.{slot_name}")
        else:
            raise AttributeError(f"{self.frame_id}.{slot_name}")

    def cook_raw_slot(self, raw_slot, format_ok=True,
                      ignore_format_errors=False):
        r'''Returns cooked value from `raw_slot`.

        Cooked means:

          - Convert slot_list to dynamic_slot_list (w/inheritance and splicing)
          - Drops leading quote (`)
          - Looks up $frame_labels
          - Formats values containing '{', if format_ok is True

        Returns everything else unchanged (including "<DELETED>").
        '''
        if isinstance(raw_slot, slot_list):
            return raw_slot.prepared()
        value = raw_slot['value']
        if value[0] == "`":
            return value[1:]
        if value[0] == "$":
            f = self.version_obj.get_frame(value[1:])
            return f.add_context(self, raw_slot.get('splices', ()))
        if format_ok and '{' in value:
            #print("cook formatting", value)
            try:
                ans = value.format_map(context(self))
            except (AttributeError, KeyError):
                if ignore_format_errors:
                    return value
                raise
            #print("cook got", repr(ans))
            #print(f"*********** {self.frame_label}.{raw_slot['name']} cooked",
            #      value, "is", ans)
            return ans
        return value


class dynamic_frame(frame):
    def __init__(self, frame_id, version_obj, raw_slots, parent=None, splices=()):
        frame.__init__(self, frame_id, version_obj, raw_slots)
        self.parent = parent      # dynamic_frame (or None)
        self.splices = splices    # [(slot_list_name, dynamic_frame), ...]

    def override_raw_slot(self, name, raw_slot):
        r'''Overrides slot `name` with raw_slot.

        This can override an existing raw_slot.  If the existing raw_slot is a
        slot_list, the entire slot_list is replaced.

        Called by slot_list.splice.
        '''
        if name.lower() == 'ako':
            raise AssertionError(
                    f"Frame {self.frame_label}: can't override_raw_slot on "
                    "'ako' slot")

        # FIX: probably don't need this:
        #if name.lower() in self.raw_slots and \
        #   isinstance(self.raw_slots[name.lower()], slot_list):
        #    # This may never get used...
        #    self.raw_slots[name.lower()].insert_raw_slot(raw_slot)
        #else:
        #    self.raw_slots[name.lower()] = raw_slot.copy()

        self.raw_slots[name.lower()] = raw_slot.copy()

    def delete_slot(self, name):
        r'''Deletes slot `name`.

        This works for multi-valued slots too (slot_lists).

        Doesn't return anything.
        '''
        # FIX: What happens if name is "ako"??
        raw_slot = self.raw_slots.get(name.lower())
        if raw_slot is None:
            raise AssertionError(
                    f"Frame {self.frame_label} does not have slot {name!r}")
        if isinstance(raw_slot, slot_list):
            raw_slot.delete_list()
        else:
            self.version_obj.delete_slot(raw_slot['slot_id'])
        del self.raw_slots[name.lower()]

    def set_slot(self, name, value, description=None):
        r'''Sets slot value (and possibly description) for slot `name`.

        If slot `name` already has a value, it is changed to the new value.
        Otherwise, a new slot is created.

        The `value` may be a list, tuple or slot_list to completely replace
        the current value (whatever it might be) with this list of values.

        Doesn't return anything.
        '''
        # FIX: What happens if name is "ako"??

        raw_slot = self.raw_slots.get(name.lower())

        if isinstance(raw_slot, slot_list):
            raw_slot.delete_list()
            raw_slot = None

        if islist(value):
            if not isinstance(raw_slot, slot_list):
                self.version_obj.delete_slot(raw_slot['slot_id'])
            assert description is None, \
                   f"Frame {self.frame_label}.{name}: " \
                   "description not allowed when updating to list"
            self.raw_slots[name.lower()] = \
              self.version_obj.create_list(self, name, value)
        elif raw_slot is None:
            # Creating a new slot...
            self.raw_slots[name.lower()] = \
              self.version_obj.create_slot(self.frame_id, name, value,
                                           description=description)
        else:
            # Update a new slot...
            slot_id = self.version_obj.update_slot(raw_slot['slot_id'], value,
                                                   description=description)
            raw_slot['slot_id'] = slot_id
            raw_slot['value'] = value
            raw_slot['description'] = description


class slot_list:
    r'''Interface to lists of values.

    Does not include inherited values.  Those are added when the slot_list is
    accessed to create a temporary dynamic_slot_list, which has the useful
    operations on the full slot_lists.
    '''
    def __init__(self, frame, name, raw_slots):
        r'''raw_slots must be sorted by slot_list_order.
        '''
        self.frame = frame
        self.name = name
        self.raw_slots = list(raw_slots)

    def __repr__(self):
        return f"<slot_list: {self.frame.frame_label}.{self.name}>"

    def iter_raw_slots(self):
        return iter(self.raw_slots)

    def prepared(self):
        return dynamic_slot_list(self.frame, self.name, self.raw_slots)


class dynamic_slot_list:
    r'''Interface to lists of values.

    Each value within the slot_list is stored as a separate slot row in the
    frames database.  The values in the slot_list are ordered by the
    slot_list_order column.

    some_slot_list[i]
    some_slot_list.get_raw_slot(i)
    some_slot_list.iter_raw_slots()
    some_slot.delete_list()
    some_slot.insert(value, i=None, description=None)
    some_slot.set_value(i, value, description=None)
    some_slot_list.splice(i, splice_frame)

    Also supports len() and iteration.
    '''
    def __init__(self, frame, name, raw_slots):
        #print("dynamic_slot_list", frame.frame_label, name)
        self.frame = frame
        self.name = name
        self.raw_slots = list(raw_slots)
        self.prepare()
        #print("dynamic_slot_list done, len", len(self))
        #print("dynamic_slot_list", self.raw_slots)

    def __str__(self):
        return f"[{', '.join(str(v['value']) for v in self.raw_slots)}]"

    def __getitem__(self, i):
        return self.frame.cook_raw_slot(self.raw_slots[i])

    def __len__(self):
        return len(self.raw_slots)

    @property
    def version_obj(self):
        return self.frame.version_obj

    def iter_raw_slots(self):
        return iter(self.raw_slots)

    def __iter__(self):
        for v in self.raw_slots:
            yield self.frame.cook_raw_slot(v)

    def get_raw_slot(self, i):
        return self.raw_slots[i]

    def prepare(self):
        #print("prepare, starting len", len(self))
        self.inherit_values()
        #print("prepare after inherit_values, len", len(self))
        self.do_splicing()
        #print("prepare after do_splicing, len", len(self))

    def inherit_values(self):
        r'''Adds inherited slot values to self.raw_slots.
        '''
        my_slot_list_orders = frozenset([slot['slot_list_order']
                                     for slot in self.raw_slots])
        for slot_list_order, slot \
         in self.frame.get_inherited_values(self.name).items():
            if slot_list_order not in my_slot_list_orders:
                self.raw_slots.append(slot)
        self.raw_slots.sort(key=itemgetter('slot_list_order'))

    def do_splicing(self):
        #print(self, "do_splicing called")
        i = 0
        while i < len(self.raw_slots):
            value = self.raw_slots[i]['value']
            if value[0] == '$':
                value = self.frame.version_obj.get_frame(value[1:])
            #print("checking index", i, "got", value)
            if isinstance(value, frame) and \
               asbool(getattr(value, 'splice', 'false')):
                #print("got splice frame at index", i)
                self.splice(i, value)
            else:
                i += 1
        #print(self, "do_splicing done")

    def splice(self, i, splice_frame):
        r'''Splices new raw_slots into current raw_slots replacing i.
        
        Makes copies of each new raw_slot, then changes their slot_list_orders.

        Returns the copied new raw_slots (in case you want to see their newly
        assigned slot_list_orders).
        '''
        splice_slot = self.raw_slots[i]

        # Look that up in the splice_frame to get the slot_list to splice into
        # myself.
        slot_list_to_splice = getattr(splice_frame, self.name, None)

        new_raw_slots = []
        if isinstance(slot_list_to_splice, dynamic_slot_list):
            for slot in slot_list_to_splice.iter_raw_slots():
                new_slot = slot.copy()
                if 'splices' not in new_slot:
                    new_slot['splices'] = [(self.name, splice_frame)]
                else:
                    new_slot['splices'].insert(0, (self.name, splice_frame))
                new_raw_slots.append(new_slot)
        self.raw_slots[i:i+1] = new_raw_slots

    def delete_list(self):
        for raw_slot in self.iter_raw_slots():
            assert not isinstance(raw_slot, slot_list), \
                   f"Found slot_list as element in slot_list"
            self.version_obj.delete_slot(raw_slot['slot_id'])

    def insert(self, value, i=None, description=None):
        r'''Inserts a new value at `i`.
        
        Inserts at end if i is None.
        '''
        if i is None:
            slot_list_order = self.get_raw_slot(-1)['slot_list_order'] + 1
            i = len(self.raw_slots)
        elif i == 0:
            slot_list_order = self.get_raw_slot(0)['slot_list_order'] - 1
        else:
            slot_list_order = (self.get_raw_slot(i - 1)['slot_list_order']
                           + self.get_raw_slot(i)['slot_list_order']) / 2
        raw_slot = self.version_obj.create_slot(self.frame.frame_id, self.name,
                                                value, slot_list_order, description)
        self.raw_slots.insert(i, raw_slot)

    def set_value(self, i, value, description=None):
        r'''Update the value at index `i`.

        Doesn't return anything.
        '''
        raw_slot = self.get_raw_slot(i)
        raw_slot['slot_id'] = \
          self.version_obj.update_slot(raw_slot['slot_id'], value,
                                       raw_slot['slot_list_order'], description)
        raw_slot['value'] = value
        raw_slot['description'] = description



if __name__ == "__main__":
    import sys
    sys.setrecursionlimit(100)

    import argparse
    from frames_db import sqlite3_db

    parser = argparse.ArgumentParser(description="Print frame")
    parser.add_argument('--database', default='frames.db')
    parser.add_argument('--user', default='bruce')
    parser.add_argument('frame_label')
    parser.add_argument('version_name')

    args = parser.parse_args()

    def print_slots(frame):
        print('slot_id', 'frame_id', 'name', 'value')
        for _, row in sorted(frame.items(), key=lambda item: item[0][1:]):
            if row['slot_list_order'] is not None:
                print(row['slot_id'], row['frame_id'],
                      f"{row['name']}[{row['slot_list_order']}]:", row['value'])
            else:
                print(row['slot_id'], row['frame_id'], f"{row['name']}:",
                      row['value'])

    with sqlite3_db().connect(args.database) \
                     .at_version(args.user, args.version_name) as version_obj:
        the_frame = version_obj.get_frame(args.frame_label)
        the_frame.dump()
