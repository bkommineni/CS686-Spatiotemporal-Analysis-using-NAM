# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I

###### For the 30% dataset provided :

###### No of records in the data set - 108000000

###### There are only three geohashes where snow depth is greater than zero for entire year :

All three places are in canada:

c41xurr50ypb    1.4427822 (Canada near to Mt. Edziza Provincial Park near to Gulf of Alaska)</br>
c1p5fmbjmkrz    0.85555875 (Canada near to Homathko river -Tatlayoko protected area)</br>
c1gyqex11wpb    0.5148892 (Canada near to Mount Pattullo)</br>

###### Hottest temperature as per dataset is observed at :

d5f0jqerq27b    Sun Aug 23 11:00:00 PDT 2015,330.67432(134°F)</br>

Considering geohash prefix of four characters above geohash falls into region of city Cancun,Mexico.
During August, the average temperature is the same as it was in June and July - 28°C/82°F, 
created by average highs of 34°C/93°F in the heat of the day and average lows of 25°C/77°F in the coolest part of the night.
June is the hottest month in Cancun with an average temperature of 28°C (82°F).
Coldest is January at 23°C (73°F) with the most daily sunshine hours at 11 in August.

###### Most frequently struck by lightning :

Everything near to Mexico

9g3h968ygj7z    156 Toluca,Mexico<br/>
9eqepuxk7x20    148 Cerro Las Conchas,Mexico</br>
d7mkkfvu34up    135 Dominian Republic</br>
9shyzwdbxnup    89  Mexico</br>
9wkkbht8spkp    87</br>
941zjqq2vtxu    84</br>

###### Driest Month in Bay Area during 2015:

List of Bay Area Geohashes:

![alt text](images/BayAreaDataPoints.png "List of Bay Area Geohash prefixes")

![alt text](images/DriestMonth.png "Avg precipitation values in Bay Area 2015")

From the histogram, we can say that April is driest month in Bay Area

###### Top 3 geohashes suitable for both solar and wind farms in North America

![alt text](images/NorthAmericaDataPoints.png "List of North America Geohash prefixes")

Geohash                                                 wind_speed             cloud_cover<br/>
dptp9djvykeb(Michigan)                                  40.09668711334537       -7.3170994E17<br/>
cbdgu8ssqukp(Border of North Dakota and Minnesota)      35.14385868738097       -7.3170994E17<br/>
cd5q18brc4gz(Canada)                                    32.80954470153044       -7.3170994E17<br/>


