# (mini) Database Management System

## Usage

**How to build:**
run `make build` to compile the code with an entrypoint to src.Main

**How to run:**
Default: `make test` will use a db_loc of "db", page_size of 1024, buffer_size of 35, and indexing of false.

Custom Args: run "java src.Main <db_loc> <page_size> <buffer_size> <indexing>"

- for <db_loc> provide the absolute path to the directory which will store the database. create this directory beforehand.
- <page_size> in bytes
- <buffer_size> is the page capacity for in memory buffer
- <indexing> should be set to false

**How to clean up:**
run `make clean` to delete all .class files in the `./src` directory.

## Project Description & My Contributions

This project was for CSCI 421 - Database System Implementation. I worked in a group of 5 teammates: myself, Duncan Small, Austin Cepalia, Tristan H, and Kevin Martin. The miniature DBMS implemented is far from full PL functionality, but does include sufficient DDL & DML parsing capabilities. See samplerun.txt for a brief example of available functionality. The B+ Tree indexing version of the DBMS is broken.

# Personal Contributions

- Created BufferManager class using a least recently used (LRU) methodology to minimize reads from disk, keeping data in memory and increasing efficiency.
- Determined robust data format for the spacing in bytes needed for storing database records and tables on disk.
- Built Record and Page classes including the logic for reading and writing database contents to disk using ByteArrays for input and output streams.
- Wrote storage manager logic for "ALTER" and "UPDATE" table commands
- Worked on storage manager logic for inserting and deleting records to/from tables.
