# frames.py

from itertools import groupby
from operator import attrgetter

import users
import versions


def get_frame_id(conn, frame_label):
    r'''Returns the frame_id for `frame_label`.

    If frame_label is an int, or an int in str format, it is simply converted
    to int and returned.

    Otherwise, it is taken as a frame_name and the frame_id for it is retrieved
    from the 'Frame' table.
    '''
    if isinstance(frame_label, str) and not frame_label.isdigit():
        #print("get_frame_id got", frame_label)
        return conn.select_1_value('Frame', 'frame_id', name=frame_label)
    return int(frame_label)


def frame_has_version(conn, frame_id, version_id=None):
    r'''True iff `frame_id` as a direct version for `version_id`.
    '''
    if version_id is None:
        version_id = conn.version_id
    return conn.exists('Frame_version',
                       frame_id=frame_id, version_id=version_id)


def get_selected_frame(conn, frame_label, version_id=None):
    r'''Returns the selected_frame row representing this `version_id`.
    '''
    if isinstance(frame_label, str) and not frame_label.isdigit():
        frame_id = get_frame_id(conn, frame_label)
    else:
        frame_id = int(frame_label)
    return selected_frame(conn, frame_id, version_id)


def selected_frame(conn, frame_id, target_version_id=None):
    r'''Returns one row of Frame_version.

    Raises AttributeError if not exactly one match.
    '''
    if target_version_id is None:
        target_version_id = conn.version_id
    print("selected_frame: frame_id",  frame_id,
          "target_version_id", target_version_id)
    conn.execute(
        'SELECT *',
        '  FROM Frame_version fv',
        ' WHERE frame_id = :frame_id',
        '   AND (version_id = :target_version_id',
        '        OR EXISTS (SELECT NULL FROM Version_subsets',
        '                    WHERE superset_id = :target_version_id',
        '                      AND subset_id = fv.version_id)',
        '   AND NOT EXISTS (',
        '             SELECT NULL',
        '               FROM Frame_version super',
        '                    INNER JOIN Version_subsets vs_down',
        '                       ON vs_down.superset_id = super.version_id',
        '                          AND vs_down.subset_id = fv.version_id',
        '              WHERE super.frame_id = fv.frame_id',
        '                AND (super.version_id = :target_version_id',
        '                     OR EXISTS (',
        '                        SELECT NULL',
        '                          FROM Version_subsets vs_up',
        '                         WHERE vs_up.superset_id = :target_version_id',
        '                           AND vs_up.subset_id = super.version_id))))',
        frame_id=frame_id,
        target_version_id=target_version_id)

    rows = conn.fetchall()
    print("rows:")
    for row in rows:
        for key in sorted(row.keys()):
            print("   ", key, row[key])
        print()
    if len(rows) > 1:
        raise AssertionError(f"Ambiguous versions for frame {frame_id}, "
                             f"target_version_id {target_version_id}, "
                             f"has {sorted(r['version_id'] for r in rows)}")
    if not rows:
        raise AssertionError(f"No version of frame {frame_id} for "
                             f"target_version_id {target_version_id}")
    return rows[0]


def slot_has_version(conn, frame_id, slot_label, version_id=None):
    r'''Returns slot_id or None.
    '''
    if version_id is None:
        version_id = conn.version_id
    if isinstance(slot_label, str):
        for slot_id in conn.select_1_column('Frame_slots', 'slot_id',
                                            frame_id=frame_id, name=slot_label,
                                            version_id=version_id,
                                            slot_list_order=None):
            return slot_id
    else:
        if conn.exists('Frame_slots',
                       frame_id=frame_id, slot_id=slot_label,
                       version_id=version_id,
                       slot_list_order=None):
            return slot_label
    return None


def get_selected_slots(conn, frame_id, slot, version_id=None):
    r'''Gets all selected slots with `slot` for `frame_id`.

    Returns Frame_slots rows.

    These are in slot_list_order.

    Raises AssertionError if there are any ambiguous slot versions.
    '''
    ans = []
    for slot_id, rows \
     in groupby(selected_slots(conn, frame_id, slot_name, version_id),
                key=attrgetter('slot_id')):
        sorted_rows = sorted(rows, key=attrgetter('version_id'))
        if len(sorted_rows) > 1:
            raise AssertionError(
                    f"Ambiguious versions for slot_id {slot_id}: "
                    f"{r['version_id'] for r in sorted_rows}")
        ans.append(sorted_rows[0])
    return sorted(ans, key=attrgetter('slot_list_order'))


def selected_slots(conn, frame_id, slot=None, version_id=None):
    r'''Read selected slots from Frame_slots.

    Executes the SQL.  Use the conn.default_cursor to read the results.

    Ambiguities not identified here.

    `slot` may omitted to get all slots, a str for the desired slot name, or
    an int for the desired slot_id.
     
    Slots are ordered by slot_id.
    '''
    if version_id is None:
        version_id = conn.version_id
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
    sql_lines.extend([
        '   AND (version_id = :target_version_id',
        '        OR EXISTS (SELECT NULL FROM Version_subsets',
        '                    WHERE superset_id = :target_version_id',
        '                      AND subset_id = fs.version_id)',
	'   AND NOT EXISTS (',
	'         SELECT NULL',
	'           FROM Slot_version super',
	'                INNER JOIN Version_subsets vs',
	'                   ON vs.superset_id = super.version_id',
	'                      AND vs.subset_id = fs.version_id',
	'          WHERE super.slot_id = fs.slot_id',
	'            AND (super.version_id = :target_version_id',
	'                 OR EXISTS (',
	'                    SELECT NULL',
	'                      FROM Version_subsets',
	'                     WHERE superset_id = :target_version_id',
	'                       AND subset_id = super.version_id))))',
	' ORDER BY slot_id',
    ])
    conn.execute(*sql_lines,
                 frame_id=frame_id,
                 target_version_id=version_id,
                 **params)


def get_inherited_slots(conn, frame_id, slot_name, version_id=None,
                        do_isa=True):
    r'''
    Returns [(slot_id, slot_list_order, value, description, version_id)].

    Returned list is in slot_list_order.

    Includes <DELETED> slots.

    Does not do splicing!
    '''
    slots = get_selected_slots(conn, frame_id, slot_name, version_id)
    if len(slots) == 1 and slots[0][1] is None:
        # 1 answer with no slot_list_order, this overrides ALL inherited slots!
        return slots

    def inherit_slots(link, do_isa):
        inh_frame_id = get_selected_frame(conn, frame_id, version_id)[link]
        if inh_frame_id is None:
            return slots

        inh_slots = get_inherited_slots(conn, inh_frame_id,
                                        slot_name, version_id, do_isa)
        if len(inh_slots) == 1 and inh_slots[0][1] is None:
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
    with conn.trans_attrs(
           user_id=users.get_user_id(conn, frames['user']),
           version_id=versions.get_version_id(conn, frames['selected_version']),
    ):
        for section in frames['frames']:
            if 'add' in section:
                load_add_frames(conn, section['add'])
            elif 'change' in section:
                load_change_frames(conn, section['change'])
            elif 'delete' in section:
                load_delete_frames(conn, section['delete'])
            else:
                raise KeyError(f"Missing change-type in {section}")


def load_add_frames(conn, frames):
    for frame in frames:
        load_add_frame(conn, frame)


def load_add_frame(conn, frame):
    r'''Returns '$xxx' frame reference.
    '''
    fields = frame.copy()
    frame_name = fields.pop('frame_name', None)
    print("adding frame", frame_name)
    isa = fields.pop('isa', None)
    if isa is not None:
        assert isinstance(isa, str) and isa[0] == '$'
        isa = get_frame_id(conn, isa[1:])
    ako = fields.pop('ako', None)
    if ako is not None:
        assert isinstance(ako, str) and ako[0] == '$'
        ako = get_frame_id(conn, ako[1:])
    description = fields.pop('frame_description', None)
    if conn.exists("Frame", name=frame_name):
        frame_id = conn.select_1_value("Frame", "frame_id", name=frame_name)
        print("reusing frame_id", frame_id, "for", frame_name)
    else:
        conn.insert("Frame",
                    name=frame_name,
                    creation_user_id=conn.user_id,
                    creation_timestamp=conn.now)
        frame_id = conn.lastrowid
        print("created new frame_id", frame_id, "for", frame_name)
    conn.insert("Frame_version",
                frame_id=frame_id,
                version_id=conn.version_id,
                isa=isa,
                ako=ako,
                description=description,
                creation_user_id=conn.user_id,
                creation_timestamp=conn.now)

    for name, value in fields.items():
        load_add_slot(conn, frame_id, name, value)

    return f"${frame_name or frame_id}"


def load_add_slot(conn, frame_id, name, value, slot_list_order=None,
                  splice_ok=False):
    r'''Returns slot_list_order used.
    '''
    if name in ('frame_name', 'isa', 'ako', 'frame_description'):
        raise ValueError(f"Illegal slot_name: {name}")

    if '[' in name:
        if splice_ok and slot_list_order is None:
            load_splice(conn, frame_id, name, value)
        else:
            raise AssertionError(f"'[' not legal in slot name {name}")
    elif islist(value):
        assert slot_list_order is None
        slot_list_order = 1000
        for v in value:
            slot_list_order = \
              load_add_slot(conn, frame_id, name, v, slot_list_order) + 1
    else:
        description = None
        while isinstance(value, dict) and 'value' in value:
            if 'slot_list_order' in value:
                slot_list_order = value['slot_list_order']
            if 'description' in value:
                description = value['description']
            value = value['value']
        if isinstance(value, dict):
            db_value = load_add_frame(conn, value)
        else:
            db_value = str(value)
        conn.insert("Slot", frame_id=frame_id,
                    creation_user_id=conn.user_id,
                    creation_timestamp=conn.now)
        conn.insert("Slot_version",
                    slot_id=conn.lastrowid,
                    version_id=conn.version_id,
                    name=name,
                    slot_list_order=slot_list_order,
                    description=description,
                    value=db_value,
                    creation_user_id=conn.user_id,
                    creation_timestamp=conn.now)
    return slot_list_order


def load_change_frames(conn, changes):
    if versions.is_frozen(conn):
        raise AssertionError(
                f"Can not make changes to frames, "
                f"version {versions.get_name(conn.version_id)} "
                f"has status {versions.get_status(conn.version_id)}")

    for change in changes:
        if len(change) != 1:
            raise AssertionError(
                    f"Only one frame per change allowed {change.keys()}")
        for frame_name, commands in change.items():
            frame_id = get_frame_id(conn, frame_name)
            print("changing", frame_id, frame_name)
            for command in commands:
                if len(command) != 1:
                    raise AssertionError(
                            f"Only one command allowed {command.keys()}")
                for command_name, slots in command.items():
                    if command_name == 'add':
                        for slot_name, value in slots.items():
                            load_add_slot(conn, frame_id, slot_name, value,
                                          splice_ok=True)
                    elif command_name == 'change':
                        slots2 = slots.copy()
                        isa = slots2.pop('isa', None)
                        ako = slots2.pop('ako', None)
                        description = slots2.pop('frame_description', None)
                        fields = {}
                        if isa is not None:
                            assert isinstance(isa, str) and isa[0] == '$'
                            fields['isa'] = get_frame_id(conn, isa[1:])
                        if ako is not None:
                            assert isinstance(ako, str) and ako[0] == '$'
                            fields['ako'] = get_frame_id(conn, ako[1:])
                        if description is not None:
                            fields['description'] = description
                        if fields:
                            if conn.exists('Frame_version',
                                           frame_id=frame_id,
                                           version_id=conn.version_id):
                                conn.update('Frame_version',
                                            dict(frame_id=frame_id,
                                                 version_id=version_id),
                                            updated_user_id=conn.user_id,
                                            updated_timestamp=conn.now,
                                            **fields)
                            else:
                                # Bring unchanged values forward from prior
                                # version:
                                base_frame = get_selected_frame(conn, frame_id)
                                for name in ('isa', 'ako', 'description'):
                                    if name not in fields:
                                        fields[name] = base_frame[name]

                                conn.insert('Frame_version',
                                            frame_id=frame_id,
                                            version_id=conn.version_id,
                                            creation_user_id=conn.user_id,
                                            creation_timestamp=conn.now,
                                            **fields)
                        for slot_name, value in slots2.items():
                            load_change_slot(conn, frame_id, slot_name, value)
                    elif command_name == 'delete':
                        for slot_name in slots:
                            load_delete_slot(conn, frame_id, slot_name)
                    else:
                        raise ValueError(f"Command must be add/change/delete, "
                                         f"got {command_name}")


def load_change_slot(conn, frame_id, name, value, do_move=True):
    if name in ('frame_name', 'isa', 'ako', 'frame_description'):
        raise ValueError(f"Illegal slot_name: {name}")

    if '[' in name:
        load_splice(conn, frame_id, name, value)
    elif islist(value):
        raise NotImplementedError(
                "changing slot to list value not yet implemented")
    else:
        # Is there already a slot_id for this version?
        slot_id = slot_has_version(conn, frame_id, name)
        if slot_id is None:
            # No, must insert new slot_id

            # Inherit description from current slot_id...
            slots = get_inherited_slots(conn, frame_id, name)
            if len(slots) == 1 and slots[0]['slot_list_order'] is None:
                desc = slots[0]['description']
            else:
                desc = None

            conn.insert('Slot_version',
                        version_id=conn.version_id,
                        name=name,
                        description=desc,
                        value=str(value),
                        creation_user_id=conn.user_id,
                        creation_timestamp=conn.now)
        else:
            # Yes, update existing slot_id
            conn.update('Slot_version',
                        dict(slot_id=slot_id, version_id=conn.version_id),
                        value=str(value),
                        updated_user_id=conn.user_id,
                        updated_timestamp=conn.now)


def load_delete_slot(conn, frame_id, slot_name):
    load_change_slot(conn, frame_id, slot_name, '<DELETED>', do_move=False)


def load_delete_frames(conn, names):
    conn.delete('Frame', name=names)

