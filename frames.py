# frames.py

from itertools import groupby
from operator import itemgetter

from frame_obj import islist


def lookup_frame_id(conn, frame_name):
    if frame_name is None:
        raise ValueError(f"Frame_name must not be None")
    return conn.select_1_value('Frame', 'frame_id', name=frame_name)


def get_selected_slots(version_obj, frame_id, slot, slot_list_order='all',
                       version_id=None, exc_on_ambiguity=True):
    r'''Gets all selected slots with `slot` for `frame_id`.

    Returns Frame_slots rows, one per slot_id.

    These are ordered by slot_name, slot_list_order.

    All slots for `frame_id` are returned if `slot` is None.

    All slot_list_orders for each slot_id are returned if slot_list_order ==
    'all'.

    Raises AssertionError if there are any ambiguous slot versions for the
    same slot_id.  Two slot versions are ambiguous if neither version is a
    superset of the other and neither are "<DELETED>".

    Returns one "<DELETED>" version per slot_id (rather than nothing).  This
    is the version with the greatest version_id.
    '''
    selected_slots(version_obj, frame_id, slot, slot_list_order, version_id)
    ans = []
    for slot_id, rows in groupby(version_obj, key=itemgetter('slot_id')):
        list_rows = list(rows)
        if len(list_rows) == 1:
            ans.append(list_rows[0])
        elif list_rows:
            sorted_rows = sorted((r for r in list_rows
                                    if r['value'] != '<DELETED>'),
                                 key=itemgetter('version_id'))
            if len(sorted_rows) > 1 and exc_on_ambiguity:
                raise AssertionError(
                        f"Ambiguious versions for "
                        f"frame_id {frame_id}, slot_id {slot_id}: "
                        f"{tuple(r['version_id'] for r in sorted_rows)}")
            if sorted_rows:
                ans.append(sorted_rows[0])
            else:
                ans.append(sorted(list_rows,
                                  key=itemgetter('version_id'),
                                  reverse=True)
                             [0])
    return sorted(ans, key=itemgetter('name', 'slot_list_order'))


def selected_slots(version_obj, frame_id, slot=None, slot_list_order='all',
                   version_id=None):
    r'''Read selected rows from Frame_slots.

    Executes the SQL.  Use the version_obj.default_cursor to read the results.

    Ambiguities not identified here.

    `slot` may omitted to get all slots, a str for the desired slot name, or
    an int for the desired slot_id.
     
    Slots are ordered by slot_id.

    <DELETED> slots are included.
    '''
    if version_id is None:
        version_id = version_obj.version_id
    sql_lines = [
	'SELECT *',
	'  FROM Frame_slots fs',
	' WHERE frame_id = :frame_id',
    ]
    params = {}
    if slot is not None:
        if isinstance(slot, str):
            sql_lines.append(
        '   AND name = :name')
            params['name'] = slot
        else:
            sql_lines.append(
        '   AND slot_id = :slot_id')
            params['slot_id'] = slot
    if slot_list_order != 'all':
        if slot_list_order is None:
            sql_lines.append(
        '   AND slot_list_order IS NULL')
        else:
            sql_lines.append(
        '   AND slot_list_order = :slot_list_order')
            params['slot_list_order'] = slot_list_order

    # AND fs.version_id is subset of target_version_id
    sql_lines.extend([
        '   AND (version_id = :target_version_id',
        '        OR EXISTS (SELECT NULL FROM Version_subsets',
        '                    WHERE superset_id = :target_version_id',
        '                      AND subset_id = fs.version_id)',
    ])

    # AND There is no other Slot_version ("super") that is a superset of
    #     fs.version_id and a subset of target_version_id
    sql_lines.extend([
	'   AND NOT EXISTS (',
	'         SELECT NULL',
	'           FROM Slot_version super',
	'                INNER JOIN Version_subsets vs',
	'                   ON vs.superset_id = super.version_id',
	'                      AND vs.subset_id = fs.version_id',
	'          WHERE super.slot_id = fs.slot_id',
        '            AND super.version_id != fs.version_id',
	'            AND (super.version_id = :target_version_id',
	'                 OR EXISTS (',
	'                    SELECT NULL',
	'                      FROM Version_subsets',
	'                     WHERE superset_id = :target_version_id',
	'                       AND subset_id = super.version_id))))',
    ])
    sql_lines.append(
	' ORDER BY slot_id')
    version_obj.execute(*sql_lines,
                  frame_id=frame_id,
                  target_version_id=version_id,
                  **params)


def get_inherited_slots(version_obj, frame_id, slot_name, version_id=None,
                        do_isa=True):
    r'''
    Returns a list of Frame_slot rows.

    Returned list is in slot_list_order.

    Includes <DELETED> slots.

    Does not do splicing!
    '''
    slots = get_selected_slots(version_obj, frame_id, slot_name, version_id)
    if len(slots) == 1 and slots[0]['slot_list_order'] is None:
        # 1 answer with no slot_list_order, this overrides ALL inherited slots!
        return slots

    def inherit_slots(link, do_isa):
        inh_frame_id = get_selected_frame(version_obj, frame_id, version_id) \
                         [link]
        if inh_frame_id is None:
            return slots

        inh_slots = get_inherited_slots(version_obj, inh_frame_id,
                                        slot_name, version_id, do_isa)
        if len(inh_slots) == 1 and inh_slots[0]['slot_list_order'] is None:
            # 1 answer with no slot_list_order, this overrides ALL inherited
            # slots!  Also overridden by any lower slots.
            if slots:
                return slots
            return inh_slots
        else:
            # merge slot values
            new_slots = []
            i = j = 0
            while i < len(slots) and j < len(inh_slots):
                base_slot = slots[i]
                inh_slot = inh_slots[j]
                if base_slot[1] <= inh_slot[1]:
                    new_slots.append(base_slot)
                    i += 1
                    if base_slot[1] == inh_slot[1]:
                        j += 1
                else:
                    new_slots.append(inh_slot)
                    j += 1
            new_slots.extend(slots[i:])
            new_slots.extend(inh_slots[j:])
            return new_slots

    # Do ako inheritance:
    slots = inherit_slots('ako', do_isa)

    if do_isa:
        # Do isa inheritance:
        return inherit_slots('isa', do_isa=False)
    return slots


def load_yaml(conn, frames):
    r'''Caller must "with" conn prior to calling `load_yaml`.

    `frames` is the top-level dict for all frame updates.
    '''
    print("load_yaml frames for", frames['user'], frames['selected_version'])
    version_obj = conn.at_version(frames['user'], frames['selected_version'],
                                  for_update=True)
    with version_obj:
        for section in frames['frames']:
            if 'add' in section:
                load_add_frames(version_obj, section['add'])
            elif 'change' in section:
                load_change_frames(version_obj, section['change'])
            elif 'delete' in section:
                load_delete_frames(version_obj, section['delete'])
            else:
                raise KeyError(f"Missing change-type in {section}")


def load_add_frames(version_obj, frames):
    for frame in frames:
        load_add_frame(version_obj, frame)


def load_add_frame(version_obj, frame):
    r'''Returns '$xxx' frame reference.
    '''
    fields = frame.copy()
    frame_name = fields.pop('frame_name', None)
    print("adding frame", frame_name)
    if frame_name is not None:
        try:
            frame_id = version_obj.get_frame_id(frame_name)
            raise AssertionError(f"frame_name {frame_name} already used")
        except NameError:
            pass
    version_obj.insert("Frame",
                       name=frame_name,
                       creation_user=version_obj.user,
                       creation_timestamp=version_obj.now)
    frame_id = version_obj.lastrowid
    print("created new frame_id", frame_id, "for", frame_name)

    for name, value in fields.items():
        load_add_slot(version_obj, frame_id, name, value)

    return f"${frame_name or frame_id}"


def load_add_slot(version_obj, frame_id, name, value, slot_list_order=None,
                  splice_ok=False):
    r'''Returns slot_list_order used.
    '''
    if name.lower() in ('frame_name',):
        raise ValueError(f"Illegal slot_name: {name}")

    if '[' in name:
        if splice_ok and slot_list_order is None:
            load_splice(version_obj, frame_id, name, value)
        else:
            raise AssertionError(f"'[' not legal in slot name {name}")
    elif islist(value):
        assert slot_list_order is None
        slot_list_order = 1000
        for v in value:
            slot_list_order = \
              load_add_slot(version_obj, frame_id, name, v, slot_list_order) + 1
    else:
        description = None
        while isinstance(value, dict) and 'value' in value:
            if 'slot_list_order' in value:
                slot_list_order = value['slot_list_order']
            if 'description' in value:
                description = value['description']
            value = value['value']
        if isinstance(value, dict):
            db_value = load_add_frame(version_obj, value)
        else:
            db_value = str(value)

        # Does slot already have a value assigned for this version?
        current_rows = get_selected_slots(version_obj, frame_id, name,
                                          slot_list_order)
        print("load_add_slot", frame_id, name, slot_list_order, current_rows)
        if current_rows:  # Can only be 0 or 1 row
            # Yes!
            old_slot = current_rows[0]
            if old_slot['value'].upper() != '<DELETED>':
                raise AssertionError(
                        f"frame_id {frame_id}.{name}[{slot_list_order}]: "
                        "Can not add slot that is already there")
            if old_slot['version_id'] == version_obj.version_id:
                # Update Slot_version
                version_obj.update("Slot_version",
                                   dict(slot_id=slot_id,
                                        version_id=version_obj.version_id),
                                   description=description,
                                   value=db_value,
                                   updated_user=version_obj.user,
                                   updated_timestamp=version_obj.now)
                return slot_list_order

        # slot_id already assigned?
        try:
            slot_id = version_obj.select_1_value("Slot", "slot_id",
                                    frame_id=frame_id,
                                    name=name,
                                    slot_list_order=slot_list_order)
        except AssertionError:
            # No, create new Slot row...
            version_obj.insert("Slot",
                               frame_id=frame_id,
                               name=name,
                               slot_list_order=slot_list_order,
                               creation_user=version_obj.user,
                               creation_timestamp=version_obj.now)
            slot_id = version_obj.lastrowid
        version_obj.insert("Slot_version",
                           slot_id=slot_id,
                           version_id=version_obj.version_id,
                           description=description,
                           value=db_value,
                           creation_user=version_obj.user,
                           creation_timestamp=version_obj.now)
    return slot_list_order


def load_change_frames(version_obj, changes):
    for change in changes:
        if len(change) != 1:
            raise AssertionError(
                    f"Only one frame per change allowed {change.keys()}")
        for frame_name, commands in change.items():
            frame_id = version_obj.get_frame_id(frame_name)
            print("changing", frame_id, frame_name)
            for command in commands:
                if len(command) != 1:
                    raise AssertionError(
                            f"Only one command allowed in {frame_name}, "
                            f"got {command.keys()}")
                for command_name, slots in command.items():
                    if command_name == 'add':
                        for slot_name, value in slots.items():
                            load_add_slot(version_obj, frame_id,
                                          slot_name, value, splice_ok=True)
                    elif command_name == 'change':
                        for slot_name, value in slots.items():
                            load_change_slot(version_obj, frame_id,
                                             slot_name, value)
                    elif command_name == 'delete':
                        for slot_name in slots:
                            load_delete_slot(version_obj, frame_id, slot_name)
                    else:
                        raise ValueError(f"Command in {frame_name} "
                                         f"must be add/change/delete, "
                                         f"got {command_name}")


def load_change_slot(version_obj, frame_id, name, value, forced=False):
    r'''Doesn't return anything.
    '''
    if name.lower() in ('frame_name',):
        raise ValueError(f"Illegal slot_name: {name}")

    if '[' in name:
        load_splice(version_obj, frame_id, name, value)
    elif islist(value):
        raise NotImplementedError(
                "changing slot to list value not yet implemented")
    else:
        description = None
        slot_list_order = None
        while isinstance(value, dict) and 'value' in value:
            if 'slot_list_order' in value:
                slot_list_order = value['slot_list_order']
            if 'description' in value:
                description = value['description']
            value = value['value']
        if isinstance(value, dict):
            db_value = load_add_frame(version_obj, value)
        else:
            db_value = str(value)

        # Is there already a slot_id for this version?
        old_slots = get_selected_slots(version_obj, frame_id, name,
                                       slot_list_order, exc_on_ambiguity=False)
        if old_slots:
            old_slot = old_slots[0]
            if old_slot['version_id'] == version_obj.version_id:
                # Yes, update existing slot_id
                version_obj.update('Slot_version',
                  dict(slot_id=old_slot['slot_id'],
                       version_id=version_obj.version_id),
                  description=description,
                  value=str(value),
                  updated_user=version_obj.user,
                  updated_timestamp=version_obj.now)
                return
        else:
            if not forced:
                raise AssertionError(
                        f"frame_id {frame_id}.{name}: "
                        "Can not change slot, doesn't already exist")

        # slot_id already assigned?
        try:
            slot_id = version_obj.select_1_value("Slot", "slot_id",
                                    frame_id=frame_id,
                                    name=name,
                                    slot_list_order=slot_list_order)
        except AssertionError:
            # No, create new Slot row...
            version_obj.insert("Slot",
                               frame_id=frame_id,
                               name=name,
                               slot_list_order=slot_list_order,
                               creation_user=version_obj.user,
                               creation_timestamp=version_obj.now)
            slot_id = version_obj.lastrowid
        version_obj.insert("Slot_version",
                           slot_id=slot_id,
                           version_id=version_obj.version_id,
                           description=description,
                           value=db_value,
                           creation_user=version_obj.user,
                           creation_timestamp=version_obj.now)
    return


def load_delete_slot(version_obj, frame_id, slot_name):
    load_change_slot(version_obj, frame_id, slot_name, '<DELETED>', forced=True)


def load_delete_frames(version_obj, names):
    version_obj.delete('Frame', name=names)


def dump(conn, frame_id, full=False):
    from versions import get_version_name

    if full:
        f = conn.select_1("Frame", frame_id=frame_id)
    else:
        f = conn.select_1("Frame", "frame_id,name", frame_id=frame_id)
    print("Frame")
    for field in f.keys():
        print(f"  {field}:", f[field])
    with conn.cursor() as cur:
        slot_fields = "slot_id,name,slot_list_order"
        if full:
            slot_fields += ",creation_user,creation_timestamp"
        sv_fields = "version_id,value,description"
        if full:
            sv_fields += ",creation_user,creation_timestamp,"  \
                          "updated_user,updated_timestamp"
        cur.select("Slot", slot_fields, frame_id=frame_id)
        for slot in cur:
            print()
            print("Slot")
            for f in slot_fields.split(','):
                print(f"  {f}: {slot[f]}")
            try:
                conn.select("Slot_version", sv_fields, slot_id=slot['slot_id'])
            except AssertionError:
                print("  No Slot_versions")
                pass
            else:
                print("  Slot_version")
                for sv in conn:
                    for f in sv_fields.split(','):
                        if f == 'version_id':
                            print(f"    {f}:",
                                  f"{sv[f]} ({get_version_name(conn, sv[f])})")
                        else:
                            print(f"    {f}: {sv[f]}")
                    print()



if __name__ == "__main__":
    import argparse
    import frames_db

    parser = argparse.ArgumentParser(description="Dump raw frame")
    parser.add_argument("--database", default="frames.db")
    parser.add_argument("--user", default="bruce")
    parser.add_argument("--full", action="store_true", default=False)
    parser.add_argument("frame")
    args = parser.parse_args()

    db = frames_db.sqlite3_db()
    with db.connect(args.database) as conn:
        if args.frame.isdigit():
            frame_id = int(args.frame)
        else:
            frame_id = lookup_frame_id(conn, args.frame)
        dump(conn, frame_id, args.full)

