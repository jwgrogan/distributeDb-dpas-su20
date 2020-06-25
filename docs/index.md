# Distributed Db using Scala and Apache Spark


[test link](https://jwgrogan.github.io/distributeDb-dpas-su20/test.html)

[Documentation]()

## Overview
An overview of the entire project describing the general purpose of the system with the relevant information.

This project encompasses a distributed database built on Scala and Apache Spark. The purpose of this database is two fold as described below.

1. Build [Spark SQL](https://jwgrogan.github.io/distributeDb-dpas-su20/docs/spark-sql-pdf.pdf) functionality to support the following:
  - **Range queries:** given a rectangle _R_ covering a certain latitude and longitude and a set of points _*P*_, find all the points within _R_
  - **Range join queries:** given a set of rectangles _*R*_ covering a certain latitude and longitude and a set of points _*P*_, find all the _(r<sub>i</sub>,s<sub>j</sub>)_ pairs such that the point is within the rectangle
  - **Distance queries:** given a point location _P_ and distance _D_ in km, find all points thatlie within the distance _D_ from _P_
  - **Distance join queries:** given two sets of points _*P<sub>1</sub>*_ and _*P<sub>2</sub>*_ and a distance _D_ in km, find all pairs _(p<sub>1i</sub>,p<sub>2j</sub>)_ such that _p<sub>1i</sub>_ is within distance _D_ of _p<sub>2j</sub>_
  
2. [Hotspot Analysis](https://jwgrogan.github.io/distributeDb-dpas-su20/docs/hotspot-analysis-pdf.pdf)

## Inputs
Documentation for every input and output used in the system. Each document should accompany each design and explain the purpose and use of each form.

## Outputs

## ?
Documentation of every file of the system, creating and update sequences of the file should be there.

## Flowchart
System flowchart describing the series of steps used in the processing of data.

