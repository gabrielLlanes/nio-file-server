rm -rf nio-file-server/build
./gradlew jar
docker build . --tag nio-file-server
