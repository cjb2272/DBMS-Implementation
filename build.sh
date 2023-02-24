#!/usr/bin/bash

S=./source

javac $S/*.java
javac -d $S/ $S/Main.java 