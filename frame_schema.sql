-- frame_schema.sql


CREATE TABLE User (
    user_id integer primary key not null,
    login varchar(40) not null,
    password bytes(128) not null,
    name varchar(80) collate nocase not null,
    email varchar(80)
);

CREATE UNIQUE INDEX User_index ON User(login);


CREATE TABLE Version (
    version_id integer primary key not null,
    name varchar(80) collate nocase not null,
    description varchar(4096),
    status varchar(40) not null default "proposed",
       -- proposed (can be changed)
       -- final (can no longer be changed)
       -- retired (basically, hidden)
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Version_index ON Version(name);


CREATE TABLE Version_requires (
    version_id integer references Version(version_id) on delete cascade
                       not null,
    required_version_id integer references Version(version_id) not null,
    creation_user_id integer references User(user_id) not null,
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


CREATE TABLE Frame (    -- only allowed update is changing name when NULL
    frame_id integer primary key not null,
    name varchar(80) collate nocase,
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Frame_name_index ON Frame(name);


CREATE TABLE Frame_version (
    frame_id integer references Frame(frame_id) on delete cascade not null,
    version_id integer references Version(version_id) on delete cascade
                       not null,
    isa integer references Frame(frame_id),  -- on delete restrict?
    ako integer references Frame(frame_id),  -- on delete restrict?
    description varchar(4096),
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Frame_version_index ON Frame_version(frame_id, version_id);


CREATE VIEW Selected_frame AS
  SELECT target_version_id, *
    FROM Frame_version fv
   WHERE version_id = target_version_id
      OR NOT EXISTS (
           SELECT NULL
             FROM Frame_version super
                  INNER JOIN Version_subsets vs_down
                     ON vs.superset_id = super.version_id
                        AND vs.subset_id = fv.version_id
            WHERE super.frame_id = fv.frame_id
              AND (super.version_id = target_version_id
                   OR EXISTS (
                      SELECT NULL
                        FROM Version_subsets
                       WHERE superset_id = target_version_id
                         AND subset_id = super.version_id)))
   ORDER BY frame_id;


CREATE TABLE Slot (     -- Can not be updated!
    slot_id integer primary key not null,
    frame_id integer references Frame(frame_id) on delete cascade not null,
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null
);

CREATE INDEX Slot_index ON Slot(frame_id);


-- Conceptually, frames have named slots (identified by frame_id, name).
-- Each conceptual slot may have multiple values (i.e, a list).  If so, there
-- are multiple rows with the same slot name, one for each of the multiple
-- values.  These are ordered by value_order.
-- Each individual value is versioned.  Thus, versioning is done between rows
-- with the same frame_id, name and value_order; i.e., for each value in a
-- multi-valued slot.
CREATE TABLE Slot_version (
    slot_id integer references Slot(slot_id) on delete cascade not null,
    version_id integer references Version(version_id) on delete cascade
                       not null,
    name varchar(80) collate nocase not null,
    value_order real,                   -- must be NULL for single-valued slots
    description varchar(4096),
    value varchar(4096) collate nocase not null,
      -- "`foo" quotes the string, so that the value is "foo" regardless of
         -- what other characters are in "foo"
      -- otherwise, "$nnnn" points to frame nnnn
         -- nnnn may be digits for the frame_id, or letters for the frame_name
      -- anything else containing a '{' is a python format string
      -- "<DELETED>" marks a deleted slot
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Slot_version_index ON Slot_version(slot_id, version_id);
CREATE INDEX Slot_version_name_index ON Slot_version(name);


-- Connects frame_id to Slot_versions
CREATE VIEW Frame_slots AS
  SELECT Slot.frame_id,
         sv.*,
         Slot.creation_user_id AS slot_creation_user_id,
         Slot.creation_timestamp AS slot_creation_timestamp
    FROM Slot
         INNER JOIN Slot_version sv USING (slot_id);


CREATE VIEW Selected_slots AS
  SELECT target_version_id, *
    FROM Frame_slots fs
   WHERE version_id = target_version_id
      OR NOT EXISTS (
           SELECT NULL
             FROM Slot_version super
                  INNER JOIN Version_subsets vs
                     ON vs.superset_id = super.version_id
                        AND vs.subset_id = fs.version_id
            WHERE super.slot_id = fs.slot_id
              AND (super.version_id = target_version_id
                   OR EXISTS (
                      SELECT NULL
                        FROM Version_subsets
                       WHERE superset_id = target_version_id
                         AND subset_id = super.version_id)))
   ORDER BY frame_id, slot_id;

