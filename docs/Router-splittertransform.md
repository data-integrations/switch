# Router

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
port to route the record to if the rule is satisfied. Refer to the table below for a list of available functions. 
``[parameter-name]`` is the parameter based on which the selected function evaluates the value of the 
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


Supported Basic Functions
-------------------------
|Display Name|Name|Description|Example|
|------------|----|-----------|-------|
|Equals|`equals`|Checks for text equality|`equals(supplierA)`|
|Not Equals|`not_equals`|Checks for text inequality|`not_equals(supplierA)`|
|Contains|`contains`|Checks if the provided parameter is a substring of the data value|`contains(supp)`|
|Not Contains|`not_contains`|Checks if the provided parameter is not a substring of the data value|`not_contains(flip)`|
|In|`in`|Checks if the data value is contained in a pipe-delimited list provided as an the parameter|`in(supplierA\|supplierB)`|
|Not In|`not_in`|Checks if the data value is not contained in a pipe-delimited list provided as an the parameter|`not_in(supplierA\|supplierB)`|
|Matches|`matches`|Checks if the data matches the provided regular expression. Supports [Java regular expression syntax](https://docs.oracle.com/javase/tutorial/essential/regex/).|`matches(.*lierA$)`|
|Not Matches|`not_matches`|Checks if the data does not match the provided regular expression. Supports [Java regular expression syntax](https://docs.oracle.com/javase/tutorial/essential/regex/).|`not_matches(.*lierA$)`|
|Starts With|`starts_with`|Checks if the provided parameter is a prefix of the data value|`starts_with(supp)`|
|Not Starts With|`not_starts_with`|Checks if the provided parameter is not a prefix of the data value|`not_starts_with(flip)`|
|Ends With|`ends_with`|Checks if the provided parameter is a suffix of the data value|`ends_with(lierA)`|
|Not Ends With|`not_ends_with`|Checks if the provided parameter is not a suffix of the data value|`not_ends_with(flip)`|
|Number Equals|`number_equals`|Checks if the data value is a number that equals the provided parameter. Supports the Java Numeric data types.|`number_equals(234523)`|
|Number Not Equals|`number_not_equals`|Checks if the data value is a number that does not equal the provided parameter. Both the data and the provided parameter should be a number. Supports the Java Numeric data types.|`number_not_equals(234523)`|
|Number Greater Than|`number_greater_than`|Checks if the data value is a number that is greater than the provided parameter. Both the data and the provided parameter should be a number. Supports the Java Numeric data types.|`number_greater_than(234523)`|
|Number Greater Than or Equals|`number_greater_than_or_equals`|Checks if the data value is a number that is greater than or equal to the provided parameter. Both the data and the provided parameter should be a number. Supports the Java Numeric data types.|`number_greater_than_or_equals(234523)`|
|Number Lesser Than|`number_lesser_than`|Checks if the data value is a number that is lesser than the provided parameter. Both the data and the provided parameter should be a number. Supports the Java Numeric data types.|`number_lesser_than(234523)`|
|Number Lesser Than or Equals|`number_lesser_than_or_equals`|Checks if the data value is a number that is lesser than or equal to the provided parameter. Both the data and the provided parameter should be a number. Supports the Java Numeric data types.|`number_lesser_than_or_equals(234523)`|
|Number Between|`number_between`|Checks if the data value is a number that is between a lower bound and an upper bound (both inclusive) specified in the provided parameter. The lower bound and upper bound should be specified as pipe-delimited in the parameter.|`number_between(234\|523)`|
|Number Not Between|`number_not_between`|Checks if the data value is a number that is not between a lower bound and an upper bound (both exclusive) specified in the provided parameter. The lower bound and upper bound should be specified as pipe-delimited in the parameter.|`number_not_between(234\|523)`|
|Date Equals|`date_equals`|Checks if the data value is a date that equals the provided parameter. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_equals(2020-05-04)`|
|Date Not Equals|`date_not_equals`|Checks if the data value is a date that does not equal the provided parameter. Both the data and the provided parameter should be a date. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_not_equals(10:15:30)`|
|Date After|`date_greater_than`|Checks if the data value is a date that is greater than the provided parameter. Both the data and the provided parameter should be a date. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_after(2020-05-02T00:03:20-07:00[America/Los_Angeles])`|
|Date After or On|`date_greater_than_or_equals`|Checks if the data value is a date that is greater than or equal to the provided parameter. Both the data and the provided parameter should be a date. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_after_or_on(10:15:30)`|
|Date Before|`date_lesser_than`|Checks if the data value is a date that is lesser than the provided parameter. Both the data and the provided parameter should be a date. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_before(2020-05-02T00:03:20-07:00[America/Los_Angeles])`|
|Date Before or On|`date_lesser_than_or_equals`|Checks if the data value is a date that is lesser than or equal to the provided parameter. Both the data and the provided parameter should be a date. The date should be specified using one of the ISO-8601 date formats indicated in the table below.|`date_before_or_on(10:15:30)`|
|Date Between|`date_between`|Checks if the data value is a date that is between a lower bound and an upper bound (both inclusive) specified in the provided parameter. The lower bound and upper bound should be specified as pipe-delimited in the parameter.|`date_between(2020-05-04\|2020-05-04)`|
|Date Not Between|`date_not_between`|Checks if the data value is a date that is not between a lower bound and an upper bound (both exclusive) specified in the provided parameter. The lower bound and upper bound should be specified as pipe-delimited in the parameter.|`date_not_between(2020-05-04\|2020-05-05)`|


Date Formats
------------
The date parameters to the Date-based functions above should be specified using one of the following ISO-8601 date 
formats. Note that you should choose the appropriate format based on the type of the routing field in the schema.

|Routing Field Type|ISO-8601 Date Format|Example|
|------------------|--------------------|-------|
|Date|YYYY-MM-dd|`2020-05-04`|
|Time Millis|hh:mm:ss|`00:03:20`|
|Time Micros|hh:mm:ss|`00:03:20`|
|Timestamp Millis|YYYY-MM-ddThh:mm:ssZ|`2020-05-02T00:03:20-07:00[America/Los_Angeles]`|
|Timestamp Micros|YYYY-MM-ddThh:mm:ssZ|`2020-05-02T00:03:20-07:00[America/Los_Angeles]`|
