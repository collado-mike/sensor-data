# Sensor Data

## Overview
This is a simple project based on the premise of a device which needs to record sensor data and a 
processing system that needs to make that sensor data queryable. 

The device may contain multiple sensors that all need to publish values at some predictable rate,
though each sensor may publish different types of numeric data (e.g., floating point vs integer).
The device may fail, causing the stream of data to end prematurely. In such cases, the stream is
immediately terminated and there may be no chance for flushing. Thus publishing sensor data as
soon as possible is paramount over any efficiency gained by buffering. This also indicates that
the output stream produced by the device may have an incomplete record at the end. 

Given this constraint, we use a simple binary output format that publishes the timestamp of the event,
the sensor id, and the value of the event. To avoid unnecessary computation, we simply
write the timestamp and value with no record or field delimiters. 

After the raw stream is complete, we rewrite that data into different files that can be queried
easily. Each sensor's data is separated from the data of the other sensors. The data is then sorted
by time and by value to make different queries very fast. Sorting by value allows us to very quickly
read the top(k) values from a file, as well as the percentile values, with constant time. The time
sorted data is ideal for simply plotting the values on a graph. 

## Coding
For this project, simple data types were used. Mostly ArrayList, HashMap, and TreeSet are used.
The code itself is very straightforward, easy to understand, and easy to maintain. Most of the 
complexity was in the design of the file formats. I aimed to optimize space, by eliminating 
boilerplate and avoiding verbose formats, such as JSON, as well as by implementing a simple 
run-length encoding in the time sorted data file. I also tried to make the file formats
splittable, by using fixed width columns. Some things I didn't have time to implement include

* merge sorting the output files
* query methods to return pXX or top(k) from data files
* iterator method for the time-sorted file, which would return a completed SensorData from the header information and the timestamp offset
* unit tests

Given more time, I would have gone with a slightly different OO design, splitting the 
QueryFileFormat into Reader and Writer, with a separate MergeSorter class. I wasn't super happy 
with the enum for the value types and I think I would have gone with different SensorData class 
implementations to avoid having to box all of the primitive values. That would have made reading 
and writing the values out to the final files much cleaner.    