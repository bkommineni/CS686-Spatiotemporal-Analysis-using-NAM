#!/bin/bash

mvn clean package

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/record_count_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.recordcount.RecordCountJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/record_count_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/snow_depth_job
hdfs dfs -rm -r -skipTrash /user/bharu/outputs/inter_snow_depth_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.snowdepth.SnowDepthJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/inter_snow_depth_job /user/bharu/outputs/snow_depth_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/hot_temp_job
hdfs dfs -rm -r -skipTrash /user/bharu/outputs/inter_hot_temp_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.hottemperature.HottestTempJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/inter_hot_temp_job /user/bharu/outputs/hot_temp_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/driest_month_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.driestmonth.DriestMonthJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/driest_month_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/inter_lightning_job
hdfs dfs -rm -r -skipTrash /user/bharu/outputs/inter_lightning_job_1
hdfs dfs -rm -r -skipTrash /user/bharu/outputs/lightning_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.lightning.MostLikelyLightningJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/inter_lightning_job /user/bharu/outputs/inter_lightning_job_1 /user/bharu/outputs/lightning_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/inter_solar_wind_job
hdfs dfs -rm -r -skipTrash /user/bharu/outputs/solar_wind_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.solarandwindfarms.SolarAndWindFarmsJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/inter_solar_wind_job /user/bharu/outputs/solar_wind_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/personal_itenary_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.personalitenary.PersonalItenaryJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/personal_itenary_job

hdfs dfs -rm -r -skipTrash /user/bharu/outputs/climate_chart_job
yarn jar target/project2-1.0.jar edu.usfca.cs.mr.climatechart.ClimateChartJob /user/bharu/inputs/nam_2015\* /user/bharu/outputs/climate_chart_job
