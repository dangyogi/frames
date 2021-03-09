# frames.py

from itertools import groupby, chain
from operator import itemgetter
from collections import defaultdict, deque

from db import connection


class frame_connection(connection):
    def get_user(self, user_name):
        self.execute("SELECT * FROM User WHERE name = :user_name",
                     user_name=user_name)
        return self.fetchone()

    def get_user_id(self, user_name):
        return self.get_user(user_name)['user_id']

    def at_versions(self, user_id, *version_names):
        r''' Returns a new version object on this db.
        '''
        return version(self, user_id, *version_names)


class context:
    def __init__(self, version_obj, frame_id):
        self.version_obj = version_obj
        self.frame_id = frame_id

    def __getitem__(self, name):
        return self.version_obj.lookup(self.frame_id, name.lower())


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

        - update_slot(slot_id, value, value_order=None, description=None)
          Returns slot_id

        - create_slot(frame_id, name, value, value_order=None, description=None)
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

        # {frame_name: frame_id}
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
        for (frame_id, name, value_order), raw_slot in raw_frames.items():
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
            for (frame_id, name, value_order), slot in raw_frames.items():
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
            - name  # value not upper cased
            - value_order
            - description
            - value
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

        Returns {(frame_id, slot_name, value_order): raw_slot}.
        '''
        matching_slot_ids = [slot_id
                             for frame_id, slot_id, value 
                              in self.select_slot_ids_by_version(where_exp,
                                                                 sql_params)]

        self.db_conn.execute("""SELECT *
                                  FROM Slot
                                 WHERE slot_id IN (::slot_ids);""",
                             slot_ids=matching_slot_ids)

        return {(row['frame_id'], row['name'].upper(), row['value_order']):
                dict(frame_id=row['frame_id'],
                     slot_id=row['slot_id'],
                     name=row['name'],
                     value_order=row['value_order'],
                     description=row['description'],
                     value=row['value'])
                for row in self.db_conn}

    def select_slot_ids_by_version(self, where_exp, sql_params={}):
        r'''Finds matching slots that are best match to my versions.

        Finds slots matching where_exp/sql_params.

        Returns [(frame_id, slot_id, value)]
        '''
        self.db_conn.execute(f"""
          WITH desired_slots(frame_id, name, value_order, slot_id, value)
            AS (SELECT frame_id, name, value_order, slot_id, value
                  FROM Slot
                 WHERE {where_exp})

          SELECT desired_slots.*, 1, version_id
            FROM desired_slots
                 INNER JOIN Slot_versions USING (slot_id)

          UNION ALL

          -- Undesired slots that might be a better version match than the
          -- desired ones (and, hence, hide the desired value)!
          SELECT frame_id, name, value_order, s.slot_id, NULL, 0, version_id
            FROM desired_slots ds
                 INNER JOIN Slot s USING (frame_id, name, value_order)
                 INNER JOIN Slot_versions v ON v.slot_id = s.slot_id
           WHERE ds.slot_id != s.slot_id;""",
          **sql_params)

        return self.select_best_matches(self.db_conn)

    def select_best_matches(self, raw_slot_rows):
        r'''Selects desired slots that are the best match to my versions.

        raw_slot_rows is (frame_id, name, value_order, slot_id, value, desired,
        version_id)

        Returns [(frame_id, slot_id, value)]
        '''
        sorted_slots = sorted(raw_slot_rows,
                              key=lambda row: (row[0], row[1].upper(),
                                               row[2], row[3]))

        matching_slot_ids = []

        # for each slot name:
        for (frame_id, name, value_order), slots \
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

        Includes inherited slots.  Does not include <deleted> slots or the "AKO"
        slot.

        Reads in all sub-frames.
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
        new_frame = frame(frame_id, self, raw_frame)
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
        parents = deque(self.parent_ids.get(frame_id, ()))
        while parents:
            try:
                return self.lookup(parents.popleft(), name)
            except KeyError:
                parents.extend(self.parent_ids.get(frame_id, ()))
        raise KeyError(f"{name} in {frame.frame_label}")

    def delete_slot(self, slot_id):
        r'''Marks slot_id as deleted.

        Doesn't return anything.
        '''
        self.db_conn.execute("""UPDATE Slot
                                   SET value = '<DELETED>'
                                 WHERE slot_id = :slot_id""",
                             slot_id=slot_id)

    def create_list(self, frame, name, values):
        r'''Creates a new set of slots, numbering value_order from 1000 by 1.

        Returns a slot_list of the newly created values.
        '''
        raw_slots = []
        for value_order, value in enumerate(values, 1000):
            raw_slots.append(self.create_slot(frame.frame_id, name, value,
                                              value_order))
        return slot_list(frame, name, raw_slots)

    def update_slot(self, slot_id, value, value_order=None, description=None):
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
                 SET value = :value, value_order = :value_order
                     description = :description,
                     updated_user_id = :user_id,
                     updated_timestamp = datetime("now"),
               WHERE slot_id = :slot_id""",
              value=value, value_order=value_order, description=description,
              slot_id=slot_id, user_id=self.user_id)
            return slot_id

        # Otherwise, create new slot for this version...
        self.db_conn.execute("""SELECT frame_id, name
                                  FROM Slot
                                 WHERE slot_id = :slot_id""",
                             slot_id=slot_id)
        frame_id, name = self.db_conn.fetchone()
        raw_slot = self.create_slot(frame_id, name, value_order, value,
                                    description)
        return raw_slot['slot_id']

    def create_slot(self, frame_id, name, value, value_order=None,
                    description=None):
        r'''Returns a raw_slot (see get_raw_frame for what a "raw_slot" is).
        '''
        if self.frozen:
            raise AssertionError("Can not make changes to frozen versions")
        if isinstance(value, frame):
            db_value = f"${value.frame_label}"
        else:
            db_value = value
        # Insert the new slot row
        self.db_conn.execute("""
          INSERT INTO Slot (frame_id, name, value_order, value, description,
                            creation_user_id, creation_timestamp)
          VALUES (:frame_id, :name, :value_order, :value, :description,
                  :creation_user_id, datetime("now"));""",
          frame_id=frame_id, name=name, value_order=value_order, value=db_value,
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
                    value_order=value_order,
                    description=description,
                    value=value)

    def load_frame(self, slots, last_frame_id=None):
        r'''Creates a new frame with the slots specified.

        `slots` is {name: value}.

        The value_order of any list/tuple values are assigned starting at 1000.
        `value_order` is always incremented by 1.

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
                              current_index=None, value_order_offset=None):
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
                - value_order       -- the offset to apply to value_order
                                       within a list
                - slot_name         -- actual slot name
                                       (overrides `slot_name` param, allows
                                        multiple values for different versions
                                        stored under different (unique) keys)
                - description       -- defaults to None

            Inner value_info objects override outer ones.  FIX: Does this make
            sense?

            Returns slot_name, version_obj, value_order_offset, value,
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
                        "value_order not allowed on single-valued "
                        f"slot {slot_name}")
            if isinstance(value, dict):   # nested frame
                last_frame_id, value = version_obj.load_frame(value,
                                                              last_frame_id)
            if new_offset is not None:
                return (slot_name, version_obj, new_offset - i, value,
                        description)
            return (slot_name, version_obj, value_order_offset, value,
                    description)
        frame_label = f"${frame_id}"
        for name, value in slots.items():
            #print("create_frame", name, value)
            slot_name, version_obj, _, value, description = \
              unwrap_value_info(name, value)
            if not isinstance(value, (list, tuple)):
                if slot_name.upper() == 'FRAME_NAME':
                    frame_label = f"${value}"
                version_obj.create_slot(frame_id, slot_name, value,
                                        description=description)
            else:
                if slot_name.upper() in ("NAME", "AKO", "FRAME_NAME", "SPLICE"):
                    raise AssertionError(
                            f"{slot_name} slot not allowed to have "
                            "multiple values")
                value_order_offset = 1000
                for i, v in enumerate(value):
                    new_name, this_version_obj, value_order_offset, v, \
                    description = \
                      unwrap_value_info(slot_name, v, version_obj, i,
                                        value_order_offset)
                    if new_name != slot_name:
                        raise AssertionError(
                                "Not allowed to change slot name in "
                                f"multi-valued slot {name}")
                    this_version_obj.create_slot(frame_id, slot_name,
                                                 v, i + value_order_offset,
                                                 description)
        return frame_id, frame_label


class frame:
    r'''Interface object for a frame.

    The inheritance has already been done prior to creating the frame object.
    This removes <deleted> slots and the "AKO" slot.

    some_frame.slot_name -> value (may be a slot_list)
    some_frame.get_raw_slot(slot_name)  # see get_raw_frame for raw slots
    some_frame.get_slot_names() -> iterable of slot_names
    '''
    def __init__(self, frame_id, version_obj, raw_frame):
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
                next_slot = next(slots_by_name, '*empty*')
                if next_slot != '*empty*':
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
    
    @property
    def frame_label(self):
        try:
            return self.cook_raw_slot(self.get_raw_slot('frame_name'))
        except AttributeError:
            return self.frame_id

    def __repr__(self):
        #if hasattr(self, 'name'):
        #    if hasattr(self, 'class_name'):
        #        return f"<frame({self.frame_label}): " \
        #               f"{self.class_name}({self.name})>"
        #    return f"<frame({self.frame_label}): {self.name}>"
        if hasattr(self, 'class_name'):
            return f"<frame({self.frame_label}): {self.class_name}>"
        return f"<frame({self.frame_label})>"

    def frozen(self):
        return self.version_obj.frozen

    def get_slot_names(self, seen_isa=False, seen_ako=False):
        r'''Returns set of slot_names.

        Includes inherited slots.  Excludes deleted slots.

        The returned names have been uppercased.
        '''
        if 'AKO' in self.raw_slots:
            ans = self.cook_raw_slot(self.get_raw_slot('ako')) \
                      .get_slot_names(seen_isa=seen_isa, seen_ako=True)
        else:
            ans = set()
        if not seen_isa and 'ISA' in self.raw_slots:
            ans.update(self.cook_raw_slot(self.get_raw_slot('isa'))
                           .get_slot_names(seen_isa=True, seen_ako=seen_ako))
        for slot_name, slot in self.raw_slots.items():
            if not isinstance(slot, slot_list) and \
               slot['value'].upper() == '<DELETED>':
                ans.discard(slot_name)
            else:
                # Don't include 'FRAME_NAME' in inherited slots!
                skip1 = slot_name == 'FRAME_NAME' and (seen_isa or seen_ako)
                skip2 = slot_name == 'AKO' and seen_isa
                if not skip1 and not skip2:
                    ans.add(slot_name)
        #print(f"{self.frame_label}.get_slot_names:", ans)
        return ans

    def __getattr__(self, slot_name):
        return self.get_slot(slot_name)

    def get_slot(self, slot_name):
        r'''`slot_name` can be any case (upper/lower).
        '''
        return self.cook_raw_slot(self.get_raw_slot_inherited(slot_name))

    def get_raw_slot_inherited(self, slot_name, try_isa=True):
        r'''Returns raw_slot, checking for inherited slots if necessary.
        '''
        #print(f"{self.frame_id}.get_raw_slot_inherited({slot_name}, {try_isa})")
        try:
            raw_slot = self.get_raw_slot(slot_name, deleted_is_error=False)
        except AttributeError:
            if slot_name.upper() != 'FRAME_NAME' and \
               (slot_name.upper() != 'AKO' or try_isa):
                if 'AKO' in self.raw_slots:
                    ako = self.cook_raw_slot(self.get_raw_slot('ako'),
                                             format_ok=False)
                    try:
                        return ako.get_raw_slot_inherited(slot_name, try_isa)
                    except AttributeError:
                        pass
                if try_isa and 'ISA' in self.raw_slots:
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

    def get_raw_slot(self, slot_name, deleted_is_error=True):
        r'''`slot_name` can be any case (upper/lower).

        Does not use inheritance to find `slot_name`.
        '''
        #print(f"{self.frame_id}.get_raw_slot({slot_name})")
        try:
            ans = self.raw_slots[slot_name.upper()]
        except KeyError:
            pass
        else:
            if not deleted_is_error or isinstance(ans, slot_list) \
               or ans['value'].upper() != '<DELETED>':
                return ans
        if slot_name.upper() != 'FRAME_NAME':
            raise AttributeError(f"{self.frame_label}.{slot_name}")
        else:
            raise AttributeError(f"{self.frame_id}.{slot_name}")

    def cook_raw_slot(self, raw_slot, format_ok=True):
        r'''Returns cooked value from `raw_slot`.

        Cooked means:

          - Drops leading `
          - Looks up $frame_labels
          - Formats values containing '{'
        '''
        if isinstance(raw_slot, slot_list):
            return raw_slot
        value = raw_slot['value']
        if value[0] == "`":
            return value[1:]
        if value[0] == "$":
            return self.version_obj.get_frame(value[1:])
        if format_ok and '{' in value:
            ans = value.format_map(context(self.version_obj, self.frame_id))
            #print(f"*********** {self.frame_label}.{raw_slot['name']} cooked",
            #      value, "is", ans)
            return ans
        return value

    def override_raw_slot(self, name, raw_slot):
        r'''Overrides slot `name` with raw_slot.

        This can override an existing raw_slot.  If the existing raw_slot is a
        slot_list, the entire slot_list is replaced.

        Called by slot_list.splice.
        '''
        if name.upper() == 'AKO':
            raise AssertionError(
                    f"Frame {self.frame_label}: can't override_raw_slot on "
                    "'ako' slot")

        # FIX: probably don't need this:
        #if name.upper() in self.raw_slots and \
        #   isinstance(self.raw_slots[name.upper()], slot_list):
        #    # This may never get used...
        #    self.raw_slots[name.upper()].insert_raw_slot(raw_slot)
        #else:
        #    self.raw_slots[name.upper()] = raw_slot.copy()

        self.raw_slots[name.upper()] = raw_slot.copy()

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
            if isinstance(value, frame) and name.upper() not in ('AKO', 'ISA'):
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
                print(f"{name}:", repr(value))

    def delete_slot(self, name):
        r'''Deletes slot `name`.

        This works for multi-valued slots too (slot_lists).

        Doesn't return anything.
        '''
        # FIX: What happens if name is "ako"??
        raw_slot = self.raw_slots.get(name.upper())
        if raw_slot is None:
            raise AssertionError(
                    f"Frame {self.frame_label} does not have slot {name!r}")
        if isinstance(raw_slot, slot_list):
            raw_slot.delete_list()
        else:
            self.version_obj.delete_slot(raw_slot['slot_id'])
        del self.raw_slots[name.upper()]

    def set_slot(self, name, value, description=None):
        r'''Sets slot value (and possibly description) for slot `name`.

        If slot `name` already has a value, it is changed to the new value.
        Otherwise, a new slot is created.

        The `value` may be a list, tuple or slot_list to completely replace
        the current value (whatever it might be) with this list of values.

        Doesn't return anything.
        '''
        # FIX: What happens if name is "ako"??

        raw_slot = self.raw_slots.get(name.upper())

        if isinstance(raw_slot, slot_list):
            raw_slot.delete_list()
            raw_slot = None

        if isinstance(value, (slot_list, list, tuple)):
            if not isinstance(raw_slot, slot_list):
                self.version_obj.delete_slot(raw_slot['slot_id'])
            assert description is None, \
                   f"Frame {self.frame_label}.{name}: " \
                   "description not allowed when updating to list"
            self.raw_slots[name.upper()] = \
              self.version_obj.create_list(self, name, value)
        elif raw_slot is None:
            # Creating a new slot...
            self.raw_slots[name.upper()] = \
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

    Each value within the slot_list is stored as a separate slot row in the
    frames database.  The values in the slot_list are ordered by the
    value_order column.

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
        self.frame = frame
        self.name = name
        self.raw_slots = list(raw_slots)
        #print("slot_list", self.raw_slots)

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
        return self.raw_slots

    def __iter__(self):
        for v in self.raw_slots:
            yield self.frame.cook_raw_slot(v)

    def get_raw_slot(self, i):
        return self.raw_slots[i]

    def populate_raw_frame(self, raw_frame):
        for raw_slot in self.raw_slots:
            key = (self.frame.frame_id, raw_slot['name'].upper(),
                   raw_slot['value_order'])
            raw_frame[key] = raw_slot

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
            value_order = self.get_raw_slot(-1)['value_order'] + 1
            i = len(self.raw_slots)
        elif i == 0:
            value_order = self.get_raw_slot(0)['value_order'] - 1
        else:
            value_order = (self.get_raw_slot(i - 1)['value_order']
                           + self.get_raw_slot(i)['value_order']) / 2
        raw_slot = self.version_obj.create_slot(self.frame.frame_id, self.name,
                                                value, value_order, description)
        self.raw_slots.insert(i, raw_slot)

    def set_value(self, i, value, description=None):
        r'''Update the value at index `i`.

        Doesn't return anything.
        '''
        raw_slot = self.get_raw_slot(i)
        raw_slot['slot_id'] = \
          self.version_obj.update_slot(raw_slot['slot_id'], value,
                                       raw_slot['value_order'], description)
        raw_slot['value'] = value
        raw_slot['description'] = description

    def splice(self, i, splice_frame):
        r'''Splices new raw_slots into current raw_slots replacing i.
        
        Makes copies of each new raw_slot, then changes their value_orders.

        Returns the copied new raw_slots (in case you want to see their newly
        assigned value_orders).
        '''
        my_raw_slot = self.raw_slots[i]

        # Get `name` of slot holding this slot_list (self)
        name_to_splice = my_raw_slot['name']  # Not upper!

        # Look that up in the splice_frame to get the slot_list to splice into
        # myself.
        slot_list_to_splice = getattr(splice_frame, name_to_splice, None)

        new_raw_slots = []
        if slot_list_to_splice is not None:
            # Calculate value_order inc value
            start = my_raw_slot['value_order']
            if i == -1 or i == len(self.raw_slots) - 1:
                inc = 1.0
            else:
                end = self.raw_slots[i + 1]['value_order']
                inc = (end - start) / (len(slot_list_to_splice) + 1)

            # Copy all of the values out of slot_list_to_splice:
            for n, raw_value in enumerate(slot_list_to_splice.iter_raw_slots(),
                                          1):
                # Don't mess up the raw_value from the slice_frame!
                new_value = raw_value.copy()

                # Reset the value_order to get it to right place in my list.
                new_value['value_order'] = start + n * inc

                if isinstance(new_value['value'], frame):
                    # Copy slots from splice_frame into new_value['value']
                    # frame.
                    for name in splice_frame.get_slot_names():
                        if name not in ('SPLICE', 'AKO', 'FRAME_NAME',
                                        name_to_splice.upper()):
                            slot_to_stuff = splice_frame.get_raw_slot(name)
                            new_value['value'].override_raw_slot(name,
                                                                 slot_to_stuff)

                new_raw_slots.append(new_value)
        self.raw_slots[i:i+1] = new_raw_slots
        return new_raw_slots


def load_yaml(db_conn, filename):
    r'''

    yaml file is a top-list of table blocks.  Each table block is a dict with
    one of the following keys.  Each of these key's value is a list of dicts,
    one per new table row.

        - users, each user row is a dict of:
            - name
            - login
            - password
            - email (optional)

        - versions, each version row is a dict of:
            - name
            - description (optional)
            - requires, list of required version names (optional)

        - frames, each frame has slot_name: value pairs as a dict

    The creation_user_id and creation_timestamp columns are automatically
    added to the tables that have them.  The user's name is specified as the
    value of a user: key in the table-level dict.

    The Version_requires table is loaded from the requires: key for each
    version row.

    The Slot_versions table is loaded from the required_versions: key for the
    table-level frames dict.

    The slot values for the frames can be simple data values, a dict for a
    sub-frame, or a list for a multi-valued slot.  Additionally, any of these
    may optionally be contained in a dict in its value: key, where the other
    possible keys are:

        - value_order, overrides the default assigned value_order (that start
          with 1000) for this, and all subsequent, slots within a list.
        - user, uses a different creation_user_id for this one slot
        - required_versions, uses different Slot_versions for this one slot
        - description
        - name, overrides the name: for this slot to allow multiple versions
          for the same slot name (which have to use different keys in the
          frame dict to remain distinct).

    Each of these is optional.
    '''
    from yaml import load
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
    with open(filename, 'r') as file:
        data = load(file, Loader=Loader)
    for objects in data:
        if 'users' in objects:
            load_users(db_conn, objects)
        elif 'versions' in objects:
            load_versions(db_conn, objects)
        elif 'frames' in objects:
            load_frames(db_conn, objects)
        else:
            raise AssertionError(f"Unknown table {objects}")
    db_conn.commit()

def load_users(db_conn, objects):
    for user in objects['users']:
        print("loading user", user['name'], end='')
        if 'email' not in user:
            user['email'] = None
        db_conn.execute("""INSERT INTO User (login, password, name, email)
                           VALUES (:login, :password, :name, :email)""",
                        **user)
        print(' -> user_id', db_conn.lastrowid)

def load_versions(db_conn, objects):
    user_name = objects['user']
    db_conn.execute("SELECT user_id FROM user WHERE name = :user_name",
                    user_name=user_name)
    user_id, = db_conn.fetchone()
    for version in objects['versions']:
        name = version['name']
        print("loading version", name, end='')
        db_conn.execute("""
          INSERT INTO Version (name, description,
                               creation_user_id, creation_timestamp)
          VALUES (:name, :description,
                  :creation_user_id, datetime("now"))""",
          name=name, description=version.get('description'),
          creation_user_id=user_id)
        version_id = db_conn.lastrowid
        for v in version.get('requires', ()):
            db_conn.execute("""
              INSERT INTO Version_requires
                (version_id, required_version_id, creation_user_id,
                 creation_timestamp)
              SELECT :version_id, version_id, :creation_user_id,
                     datetime("now")
                FROM Version
               WHERE name = :v""",
              version_id=version_id, v=v, creation_user_id=user_id)
        print(' -> version_id', version_id)


def load_frames(db_conn, objects):
    user_id = db_conn.get_user_id(objects['user'])
    version_obj = db_conn.at_versions(user_id, *objects['required_versions'])
    for slots in objects['frames']:
        print("loading frame", slots.get('frame_name') or slots.get('name'),
              end='')
        frame_id, _ = version_obj.load_frame(slots)
        print(' -> frame_id', frame_id)


def sqlite3_conn(database_name='frames.db'):
    import sqlite3
    from db import db

    def add_row_factory(conn):
        conn.db_conn.row_factory = sqlite3.Row
        conn.reset_cursor()
    db_obj = db(sqlite3, post_connect=add_row_factory)
    db_obj.set_connection(frame_connection)
    return db_obj.connect(database_name)



if __name__ == "__main__":
    import sys
    sys.setrecursionlimit(100)

    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser_load = subparsers.add_parser('load')
    parser_load.add_argument('filenames', nargs='+')
    parser_load.set_defaults(command='load')
    for command in ('get_raw', 'get_frame'):
        subparser = subparsers.add_parser(command)
        subparser.add_argument('frame_label')
        subparser.add_argument('versions', nargs='+')
        subparser.set_defaults(command=command)

    args = parser.parse_args()

    db_conn = sqlite3_conn()

    if args.command == 'load':
        for filename in args.filenames:
            print("******* loading", filename)
            load_yaml(db_conn, filename)
    else:
        user_id = db_conn.get_user_id('bruce')
        version_obj = db_conn.at_versions(user_id, *args.versions)

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

        if args.command == 'get_frame':
            the_frame = version_obj.get_frame(args.frame_label)
            the_frame.dump()
        else:
            frame_id, raw_frame = version_obj.get_raw_frame(args.frame_label)
            if args.command == 'get_raw':
                print_slots(raw_frame)

