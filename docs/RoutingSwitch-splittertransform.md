# Routing Switch

Description
-----------
This transform routes a record to an appropriate port based on the evaluation of a simple function on the value of one
of its fields. It is implemented as a Splitter Transform.

Use case
--------
Often times, you have data feeds coming in where the value of a field in the feed typically determines the processing
that must happen on that record.
E.g. Consider a supply chain feed containing inventory information, which contains a field called *supplier_id*.
Your supply chain optimization pipeline must process records differently, based on their *supplier_id*. In this case,
you can use this plugin to set up a pipeline that routes records to different processing branches based on their
*supplier_id*.

Properties
----------
**Routing Field**: Specifies the field in the input schema on which the rules in the _Port Specification_ should be
applied, to determine the port where the record should be routed to.

**Port Specification**: Specifies the rules to determine the port where the record should be routed to. Rules are
applied on the value of the routing field. The port specification is expressed as a comma-separated list of rules,
where each rule has the format ``[port-name]:[function-name]([parameter-name])``. ``[port-name]`` is the name of the 
port to route the record to if the rule is satisfied. ``[function-name]`` can be one of _equals_, _not_equals_, 
_contains_, _not_contains_, _in_, _not_in_, _matches_, _not_matches_, _starts_with_, _not_starts_with_, _ends_with_, 
_not_ends_with_. ``[parameter-name]`` is the parameter based on which the selected function evaluates the value of the 
routing field.

**Default handling**: Determines the way to handle records whose value for the field to match on doesn't match any of
the rules defined in the port specification. Defaulting records can either be skipped ("Skip"), sent to a specific port
("Send to default port"), or sent to the error port ("Send to error port"). By default, such records are sent to the 
error port.

**Default Port**: Determines the port to which records that do not match any of the rules in the port specification
are routed. This is only used if default handling is set to "Send to default port". Defaults to 'Default'.

**Null Handling**: Determines the way to handle records whose value for the routing field is null. Such records can 
either be skipped ("Skip"), sent to a specific port ("Send to null port"), or sent to the error port 
("Send to error port"). By default, such records are sent to the error port.

**Null Port**: Determines the port to which records with null values for the field to split on are sent. This is only
used if default handling is set to "Send to null port". Defaults to 'Null'.
