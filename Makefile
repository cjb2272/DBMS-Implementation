build:
	javac ./src/*.java
	javac -d ./src/ ./src/Main.java 

test:
	java src.Main "db" 1024 35

all: build test