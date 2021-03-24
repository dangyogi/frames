This implements a general-purpose versioned frame/slot model using a
relational database for storage and retrieval.

All names and values in the database (not the python code) are case insensitive.

The slot values are limited to strings.  Thus 22 must be stored as "22".  It
is up to the application to convert to/from strings when necessary.


Special slot names:

    - "frame_name" -- tags frame for reference by other frames
    - "isa"        -- points to frame (class) describing what this frame is
    - "ako"        -- points to superclass
    - "class_name" -- names a category of things (class)

      - Declared in frames used as classes.  Inherited like normal slots,
        so that with:

            frame_name: dog
            class_name: dog

            frame_name: Fido
            isa: $dog
      
        Fido.class_name will be inherited (as "dog").  This tells what kind of
        thing you have.
    - "splice"     -- if "True", indicates that this frame represents a group
                      of values to splice into all slot_lists that contain
                      this frame.


Special values:

    - "`xxx"  -- The ` quotes the rest of the string (turns off all the rest
                 of these special values).
    - "$xxx"  -- Frame reference.  xxx may either be a frame_id, or frame_name.
    - "xx{name.slot}yy"  -- Python format string:
      
      - context for the format is a map containing the following names
        (case insensitive):
        - "frame" for the direct frame containing this value.
        - all containing frames (recursively), keyed by their "class_name"
          slot (which could be inherited).
      - The format string is applied to the context each time the slot is
        accessed.


Inheritance:

    - Supports both 'isa' and 'ako' (A-Kind-Of) inheritance.  For example:
      
      - dog has "ako" $mammal
      - Fido has "isa" $dog

    - Only one 'isa' link will be traversed searching for slots not found in
      the frame at hand.  However there is no limit to the number of 'ako'
      links traversed.  Both of these must be directly in the frame inheriting
      the values (i.e., an inherited 'ako' value is not used here).

    - Values inherited from 'ako' override those inherited from 'isa' (if one
      frame directly contains both kinds of links).

    - The special slot "frame_name" tags the frame containing it with a name
      that may be used to reference that frame.

    - "frame_name" is the only slot that is never inherited.

    - "isa" is inherited normally, but "ako" is not inherited across an "isa"
      link.


Slot Lists

These are multi-valued slots, which are treated as ordered lists.  Each value
in the slot list is separately versioned.  The values are ordered by the
"slot_list_order" column in the "Slot" database table.

Each value in a slot_list is separately inherited.  The final sort order
combines all of their slot_list_orders.  One value hides another value only if
they share the same slot_list_order.

Also each value in a slot may be separately deleted.

Finally, each value in a slot_list that is a frame object with a "splice"
slot of True, that frame is spliced into the slot list as follows:

    - A slot of the same name as the slot_list being spliced into is taken as
      the values to splice in.  For example, given:

          frame_name: a_splice
          splice: true
     +--> some_list:
     |      - a
     |      - b
     |    
     |    frame_name: some_frame
     +--- some_list:
            - x
            - $a_splice
            - y

      some_frame.some_list would be: ['x', 'a', 'b', 'y']

    - These values are spliced into the slot_list, replacing the splice frame.

    - All but the following slots from the splice frame are injected into all
      of the frames spliced into the slot_list (overriding any prior values
      that they may have had):
       
        - frame_name
        - class_name
        - isa
        - ako
        - splice
        - slot_list's name



YAML load files
