from pathlib import Path
import os
import csv

file_path = "/Users/bharu/CS686-PROJECTS/p2-bkommineni/values.txt"
cloud_cover = []
wind_speed = []
with open(file_path) as f:
        for line in f:
            cloud_cover.append(line.split("\t")[0].split(",")[1])
            wind_speed.append(line.split("\t")[1].split(",")[1].strip(' \t\n\r'))

my_file = Path("/Users/bharu/CS686-PROJECTS/p2-bkommineni/values.csv")
if my_file.exists() :
    os.remove("/Users/bharu/CS686-PROJECTS/p2-bkommineni/values.csv")

with open("/Users/bharu/CS686-PROJECTS/p2-bkommineni/values.csv", "a") as cf:
    csvwriter = csv.writer(cf)
    csvwriter.writerow(["CloudCover","WindSpeed"])

with open(my_file, "a") as cf:
    csvwriter = csv.writer(cf)
    for i in range(0, len(cloud_cover)):
        csvwriter.writerow([cloud_cover[i],wind_speed[i]])
