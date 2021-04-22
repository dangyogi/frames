-- frame_schema.sql


CREATE TABLE Version (
    version_id integer primary key not null,
    name varchar(80) collate nocase not null,
    description varchar(4096),
    status varchar(40) not null default "proposed",
       -- proposed (can be changed)
       -- final (can only be changed to retired)
       -- retired (basically, hidden final, can only be changed back to final)
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null,
    updated_user varchar(100),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Version_index ON Version(name);


CREATE TABLE Version_requires (
    -- required_version_ids, for each version_id, can not be changed
    version_id integer references Version(version_id)
                       on delete cascade not null,
    required_version_id integer references Version(version_id) not null,
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null
);

CREATE UNIQUE INDEX Version_requires_index
    ON Version_requires(version_id, required_version_id);


CREATE TABLE Version_subsets (
    superset_id integer references Version(version_id) on delete cascade
                        not null,
    subset_id integer references Version(version_id) not null,
    primary key (superset_id, subset_id)
);

CREATE INDEX Version_supersets_index
    ON Version_subsets(superset_id);

CREATE INDEX Version_subsets_index
    ON Version_subsets(subset_id);


-- Maps frame_names to frame_ids.  Also assigns new frame_ids.
CREATE TABLE Frame (    -- only allowed update is changing name when NULL
    frame_id integer primary key not null,
    name varchar(80) collate nocase,
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null,
    updated_user varchar(100),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Frame_name_index ON Frame(name);


CREATE TABLE Frame_version (
    frame_id integer references Frame(frame_id) on delete cascade not null,
    version_id integer references Version(version_id)
                       on delete cascade not null,
    isa integer references Frame(frame_id),  -- on delete restrict?
    ako integer references Frame(frame_id),  -- on delete restrict?
    description varchar(4096),
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null,
    updated_user varchar(100),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Frame_version_index ON Frame_version(frame_id, version_id);


-- Each slot_id can hold only a single value.  Multi-valued "slots" (lists) are
-- multiple slot_ids that share the same frame_id (here, in this table) and
-- slot name (in the Slot_version table).  They are distinguished by their
-- slot_list_order (also in the Slot_version table).
CREATE TABLE Slot (     -- Can not be updated!
    slot_id integer primary key not null,
    frame_id integer references Frame(frame_id) on delete cascade not null,
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null
);

CREATE INDEX Slot_index ON Slot(frame_id);


-- Each slot_id is versioned.
--
-- Conceptually, frames have named slots (identified by frame_id, name).
--
-- Each conceptual slot may have a single value, or multiple values (i.e,
-- a list).
-- 
-- Multi-valued slots use multiple rows, with different slot_ids but the same
-- slot name, one for each of the multiple values.  These are ordered by
-- slot_list_order.
--
-- Thus, each individual value is individually versioned.
--
-- Single valued slots are identified by a slot_list_order of NULL.
CREATE TABLE Slot_version (
    slot_id integer references Slot(slot_id) on delete cascade not null,
    version_id integer references Version(version_id) on delete cascade
                       not null,
    name varchar(80) collate nocase not null,
    slot_list_order real,              -- must be NULL for single-valued slots
    description varchar(4096),
    value varchar(4096) collate nocase not null,
      -- "`foo" quotes the string, so that the value is "foo" regardless of
         -- what other characters are in "foo"
      -- otherwise, "$nnnn" points to frame nnnn
         -- nnnn may be digits for the frame_id, or letters for the frame_name
      -- anything else containing a '{' is a python format string
      -- "<DELETED>" marks a deleted slot (case insensitive)
    creation_user varchar(100) not null,
    creation_timestamp timestamp not null,
    updated_user varchar(100),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Slot_version_index ON Slot_version(slot_id, version_id);
CREATE INDEX Slot_version_name_index ON Slot_version(name);


-- Connects frame_id to Slot_versions
CREATE VIEW Frame_slots AS
  SELECT Slot.frame_id,
         sv.*,
         Slot.creation_user AS slot_creation_user,
         Slot.creation_timestamp AS slot_creation_timestamp
    FROM Slot
         INNER JOIN Slot_version sv USING (slot_id);

