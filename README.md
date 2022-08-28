### Distributed Database Systems

#### 1. <ins> Database Sharding </ins>
Simulate data partitioning approaches on-top of an open source relational database
management system (PostgreSQL) and build a simplified query processor that access data from the
generated partitions. Functions implemented - 
- *rangePartition(), roundRobinPartition()*
- *roundRobinInsert(), rangeInsert()*
- *rangeQuery(), pointQuery()*

#### 2. <ins> Multithreaded Sort and Join </ins>
Implement generic parallel sort and parallel join algorithms for an RDBMS. Functions implemented - 
- *parallelSort(), parallelJoin()*

#### 3. <ins> Map Reduce </ins>
Write a map-reduce program using Hadoop MapReduce framework that performs equijoin on input from a file.

#### 4. <ins> Hotspot Analysis </ins>
- Analyzed NYC taxi trip data on Apache spark distributed system running on HDFS. 
- Performed geospatial data analysis using Spark SQL by performing point query, range query, Hot zone, and Hot cell analysis.


#### 5. <ins> NoSQL database </ins>
Perform some textual and spatial searching on MongoDB. Functions implemeted -
- *FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection)*
- *FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection)*
