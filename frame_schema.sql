-- frame_schema.sql


CREATE TABLE User (
    user_id integer primary key not null,
    login varchar(40) not null,
    password bytes(128) not null,
    name varchar(80) not null,
    email varchar(80)
);

CREATE UNIQUE INDEX User_index ON User(login);


CREATE TABLE Version (
    version_id integer primary key not null,
    name varchar(80) not null,
    name_upper varchar(80) not null,
    description varchar(4098),
    status varchar(40) not null default "proposed",
       -- proposed (can be changed)
       -- final (can no longer be changed)
       -- retired (basically, hidden)
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Version_index ON Version(name_upper);


CREATE TABLE Version_requires (
    version_id integer references Version(version_id) not null,
    required_version_id integer references Version(version_id) not null,
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Version_requires_index
    ON Version_requires(version_id, required_version_id);


CREATE TABLE Enum_type (
    enum_id integer primary key not null,
    name varchar(80) not null,
    name_upper varchar(80) not null,
    description varchar(4098),
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Enum_type_index ON Enum_type(name_upper);

CREATE TABLE Enum_option (
    enum_option_id integer primary key not null,
    enum_id integer references Enum_type(enum_id) not null,
    name varchar(80) not null,
    name_upper varchar(80) not null,
    description varchar(4098),
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE UNIQUE INDEX Enum_option_index ON Enum_option(enum_id, name_upper);


-- Conceptually, frames have named slots (identified by frame_id, name).
-- Each conceptual slot may have multiple values (i.e, a list).  If so, there
-- are multiple rows with the same slot name, one for each of the multiple
-- values.  These are ordered by value_order.
-- Each individual value is versioned.  Thus, versioning is done between rows
-- with the same frame_id, name and value_order; i.e., for each value in a
-- multi-valued slot.
CREATE TABLE Slot (
    slot_id integer primary key not null,
    frame_id integer not null,
    name varchar(80) not null,
    name_upper varchar(80) not null,
    value_order real,                   -- must be NULL for single-valued slots
    description varchar(4098),
    value varchar(4096) not null,
      -- ">nnnn" points to frame nnnn
         -- nnnn may be digits for the frame_id, or letters for the frame_name
      -- anything containing a { is a format string
      -- "<deleted>" marks a deleted slot
    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp
);

CREATE INDEX Slot_index ON Slot(frame_id, name_upper);

CREATE TABLE Slot_versions (
    slot_id integer references Slot(slot_id) not null,
    version_id integer references Version(version_id) not null,

    creation_user_id integer references User(user_id) not null,
    creation_timestamp timestamp not null,
    updated_user_id integer references User(user_id),
    updated_timestamp timestamp,

    PRIMARY KEY (slot_id, version_id)
);
