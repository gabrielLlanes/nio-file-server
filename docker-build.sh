#run in same directory
./gradlew jar
docker build . --tag nio-file-server
