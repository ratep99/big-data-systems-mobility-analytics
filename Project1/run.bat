docker build --rm -t bde/spark-app .
docker run --name projekat1 --net bde -p 4040:4040 -d bde/spark-app