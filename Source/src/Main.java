import java.util.ArrayList;
import java.util.Arrays;

public class Main {
    //java Main <db loc> <page size> <buffer size>
    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("Usage is java Main <db loc> <page size> <buffer size>");
            System.out.println(args.length);
            return;
        }

        QueryParser pas = new QueryParser();
        ArrayList<String> commands = new ArrayList<>(Arrays.asList( "display adsfschema;" , "SELECT * FROM table1;", "display info tabl2;", "display schema;"));
        for(String s : commands){
            System.out.println(s);
            pas.CommandParse( s );
            System.out.println("-------------------");
        }
    }
}