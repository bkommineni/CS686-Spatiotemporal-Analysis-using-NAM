# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I

For the 30% dataset provided :

No of records in the data set - 108000000

There are only three geohashes where snow depth is greater than zero for entire year :

All three places are in canada:

c41xurr50ypb    1.4427822 (Canada near to Mt. Edziza Provincial Park near to Gulf of Alaska)
c1p5fmbjmkrz    0.85555875 (Canada near to Homathko river -Tatlayoko protected area)
c1gyqex11wpb    0.5148892 (Canada near to Mount Pattullo)

Hottest temperature as per dataset is observed at :

d5f0jqerq27b    Sun Aug 23 11:00:00 PDT 2015,330.67432(134°F)

Considering geohash prefix of four characters above geohash falls into region of city Cancun,Mexico.
June is the hottest month in Cancun with an average temperature of 28°C (82°F).
Coldest is January at 23°C (73°F) with the most daily sunshine hours at 11 in August.
So based on the above information, looks like the result of hottest temperature is an anomaly during
December month which is supposed to be coldest time.

Most strcuk by lightning :

Everything near to Mexico

9g3h968ygj7z    156 Toluca , Mexico
9eqepuxk7x20    148 Cerro Las Conchas , Mexico
d7mkkfvu34up    135 Dominian Republic
9shyzwdbxnup    89  Mexico
9wkkbht8spkp    87
941zjqq2vtxu    84

![alt text](images/DriestMonth.png "Avg precipitation values in Bay Area 2015")


