package src;/*
 * This is the main entry to our program that will be a database manager.
 * @author Duncan Small, Austin Cepalia
 */

import java.io.File;
import java.io.IOException;
import java.util.*;

import src.*;

public class Main {

    public static int pageSize; //these values should be final
    public static int bufferSizeLimit;
    public static String db_loc;
    //java Main <db loc> <page size> <buffer size>
    public static void main(String[] args) {
        if(args.length != 3){
            System.out.println("Usage is java Main <db loc> <page size> <buffer size>");
            System.out.println(args.length);
            return;
        }
        db_loc = args[0]; // this is expected to be a folder path. No empty folder is created.
        pageSize = Integer.parseInt(args[1]);
        bufferSizeLimit = Integer.parseInt(args[2]);

        System.out.println("Looking at " + db_loc + "for existing db...");

        File catalog = new File(db_loc, "db-catalog.catalog");
        if (!catalog.isFile()) {
            try {
                System.out.println("No existing db found");
                System.out.println("Creating new db at " + db_loc);
                catalog.createNewFile();
                Catalog.instance = new Catalog(pageSize, db_loc);
                System.out.println("New db created successfully");
            } catch(IOException e) {
                System.out.println("Error in creating catalog file.");
                e.printStackTrace();
            }
        } else {
            System.out.println("Database found...");
            System.out.println("Restarting the database...");
            Catalog.instance = Catalog.readCatalogFromFile(db_loc);
            pageSize = Catalog.instance.getPageSize(); //reset so we retain page original db page size
            System.out.println("\tIgnoring provided pages size, using stored page size");

        }
        src.StorageManager.instance = new StorageManager(db_loc);

        System.out.println("Page size: " + pageSize);
        System.out.println("Buffer size: " + bufferSizeLimit);

        System.out.println("\nPlease enter commands, enter <quit> to shutdown the db.\n");

        Scanner scanner = new Scanner( System.in );

        QueryParser parser = new QueryParser();
        while(true){
            System.out.print("JottQL $ ");
            String input = "";
            while(!input.endsWith( "; " ) && !input.equals( "<quit> " ) ) {
                //needs to do this to ensure spacing is correct when inputting multiline commands.
                input += scanner.next().trim() + " ";
            }

            if(input.trim().equals( "<quit>" )){
                System.out.println("Exiting the database...");
                Catalog.instance.writeCatalogToFile();
                StorageManager.instance.writeOutBuffer();
                break;
            }

            Query query = parser.CommandParse( input );

            if (query != null) {
                // Individual query objects all have an execute method that defines what steps should be taken to execute
                // that query. Having that logic here (switched on the query type) would be a code smell.
                query.execute();  // where all the magic happens!

            }
            else{
                System.out.println("ERROR\n");
            }

    
        }
    }
}