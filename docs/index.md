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

## Inputs
Data is drawn from monthly NYC taxi trip datasets from 2009 - 2012 (e.g. "yellow_tripdata_2009-01_point.csv", "yellow_tripdata_2010-02_point.csv", etc.). These files are loaded and parsed by the code to pass inputs to the logic as described below.

---
**Function**:range 
**Description**: asdf 
**Format**: asdf
---

Range/Range join queries | file line containg a point lat-long string and a rectangle lat-long String |  _pointString:String, queryRectangle:String_
- **Distance/Distance join queries:** file line containg two point lat-long Strings and a distance as Double with format _pointString1:String, pointString2:String, distance:Double_
- **Hotzone analysis:** file line containg a point lat-long String and a rectangle lat-long String with format _pointString:String, queryRectangle:String_
- **Hotcell analysis:** file line containg a point lat-long String and a rectangle lat-long String with format _pointString:String, queryRectangle:String_

## Outputs

- **Range/Range join queries:** 
- **Distance/Distance join queries:** 
- **Hotzone analysis:** file containing all zones identified by their lat-long coordinates and the point count per zone sorted by zone an ascending order - e.g. "-155.940114,19.081331,-155.618917,19.5307", 3
- **Hotcell analysis:** 

## need this?
Documentation of every file of the system, creating and update sequences of the file should be there.

## Flowchart
System flowchart describing the series of steps used in the processing of data.

