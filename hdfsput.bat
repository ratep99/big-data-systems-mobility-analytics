@echo off
docker cp denverMobility.py namenode:/data 
docker cp data/denverVehiclesCleaned.csv namenode:/data 
docker exec -it namenode bash -c "hdfs dfs -mkdir /dir"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/denverMobility.py"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/denverVehiclesCleaned.csv"
docker exec -it namenode bash -c "hdfs dfs -put /data/denverMobility.py /dir"
docker exec -it namenode bash -c "hdfs dfs -put /data/denverVehiclesCleaned.csv /dir"