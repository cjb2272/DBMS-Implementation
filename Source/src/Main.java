/**
 * This is the main entry to our program that will be a database manager.
 * @author Duncan Small
 */

import java.util.*;

public class Main {
    //java Main <db loc> <page size> <buffer size>
    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("Usage is java Main <db loc> <page size> <buffer size>");
            System.out.println(args.length);
            return;
        }

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

            if(input.equals( "<quit> " )){
                System.out.println("Exiting the database...");
                break;
            }

            Query temp = parser.CommandParse( input );

            if(temp == null){
                System.out.println("ERROR");
            } else{
                System.out.println("SUCCESS");
            }

        }
    }
}