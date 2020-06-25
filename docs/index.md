# Distributed Db using Scala and Apache Spark


[test link](test.md)

[Documentation]()

## Overview
This project encompasses a distributed database built on Scala and Apache Spark. The purpose of this database is two fold as described below.

1. Build [Spark SQL](spark-sql-pdf.pdf) functionality to support the following:
  - **Range queries:** given a rectangle _R_ covering a certain latitude and longitude and a set of points _**P**_, find all the points within _R_
  - **Range join queries:** given a set of rectangles _**R**_ covering a certain latitude and longitude and a set of points _*P*_, find all the _(r<sub>i</sub>,s<sub>j</sub>)_ pairs such that the point is within the rectangle
  - **Distance queries:** given a point location _P_ and distance _D_ in km, find all points thatlie within the distance _D_ from _P_
  - **Distance join queries:** given two sets of points _**P<sub>1</sub>**_ and _**P<sub>2</sub>**_ and a distance _D_ in km, find all pairs _(p<sub>1i</sub>,p<sub>2j</sub>)_ such that _p<sub>1i</sub>_ is within distance _D_ of _p<sub>2j</sub>_
  
  
2. Conduct [Hotspot Analysis](hotspot-analysis-pdf.pdf) to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it includes more points. So this task is to calculate the hotness of all the rectangles.

## Data
The input will be a monthly taxi trip dataset from 2009 - 2012. For example, "yellow_tripdata_2009-01_point.csv", "yellow_tripdata_2010-02_point.csv"

## Inputs
Documentation for every input and output used in the system. Each document should accompany each design and explain the purpose and use of each form.
- **Range/Range join queries:** pointString:String, queryRectangle:String
- **Distance/Distance join queries:** pointString1:String, pointString2:String, distance:Double
- **Hotzone analysis:** pointString:String, queryRectangle:String
- **Hotcell analysis:** pointString:String, queryRectangle:String

## Outputs

## ?
Documentation of every file of the system, creating and update sequences of the file should be there.

## Flowchart
System flowchart describing the series of steps used in the processing of data.

