*mapper class*
set join column as key and single line of input as value

*reducer class*
for each key (which is join column) place the rows in two different hash sets. Place similar table rows in one set and another table rows in another set. Now concatenate rows in both sets using nested for loops. Set concatenated string as key and null as value.

*main function*
Set properties of JobConf object (data type of output key-value, mapper and reducer class name and input/output paths passed from command line). Finally call runJob() function to start map reduce process.