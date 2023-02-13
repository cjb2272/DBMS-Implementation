package src;/*
 * This is the main entry to our program that will be a database manager.
 * @author Duncan Small
 */

import java.util.*;

public class Main {

    public static int pageSize; //these values should be final
    public static int bufferSizeLimit;
    //java Main <db loc> <page size> <buffer size>
    public static void main(String[] args) {
        if(args.length != 3){
            System.out.println("Usage is java Main <db loc> <page size> <buffer size>");
            System.out.println(args.length);
            return;
        }
        String db_loc = args[0]; // do we expect this to be an empty folder, or make a new empty folder within here?
        pageSize = Integer.parseInt(args[1]);
        bufferSizeLimit = Integer.parseInt(args[2]);

        StorageManager storageManager = new StorageManager(db_loc);

        System.out.println("\nPlease enter commands, enter <quit> to shutdown the db.\n");

        Scanner scanner = new Scanner( System.in );

        QueryParser parser = new QueryParser(storageManager);
        while(true){
            System.out.print("JottQL $ ");
            String input = "";
            while(!input.endsWith( "; " ) && !input.equals( "<quit> " ) ) {
                //needs to do this to ensure spacing is correct when inputting multiline commands.
                input += scanner.next().trim() + " ";
            }

            if(input.equals( "<quit> " )){
                System.out.println("Exiting the database...");
                break;
            }

            Query query = parser.CommandParse( input );

            if(query == null){
                System.out.println("ERROR");
            } else{
                System.out.println("SUCCESS");
                query.execute();  // where all the magic happens!

            }

        }
    }
}