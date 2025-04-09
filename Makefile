build:
	javac ./src/*.java
	javac -d ./src/ ./src/Main.java 

clean:
	rm ./src/*.class
	rm -R ./src/src/

test:
	java src.Main "db" 1024 35 false

all: build test