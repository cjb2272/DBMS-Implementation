# csci421-group-2
Group 2 project for CSCI421-2225

How to build:
    run `make build` to compile the code with an entrypoint to src.Main

How to run:
    Default: `make test` will use a db_loc of "db", page_size of 1024, and buffer_size of 35

    Custom Args: run "java src.Main <db_loc> <page_size> <buffer_size>"

How to clean up:
    run `make clean` to delete all .class files in the `./src` directory.