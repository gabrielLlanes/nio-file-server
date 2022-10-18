rm -r app/build lib/build
./gradlew jar
docker build . --tag nio-file-server
