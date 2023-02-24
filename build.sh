#!/usr/bin/bash

S=./src

javac $S/*.java
javac -d $S/ $S/Main.java 