import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

class QueryParser{
    public QueryParser(){

    }

    //display info <table>;
    //display schema;
    public static DisplayQuery ParseDisplay(String input){
        String[] tokens = input.split( " " );

        switch (tokens.length){
            case 2:
                if(!tokens[1].endsWith( ";" )){
                    System.out.println("Missing Semicolon");
                    return null;
                }
                if(tokens[1].toLowerCase( Locale.ROOT ).equals( "schema;" )){
                    return new DisplayQuery();
                }
                System.out.println("Error in display format.");
                break;
            case 3:
                if(!tokens[2].endsWith( ";")){
                    System.out.printf( "Missing Semicolon." );
                    return null;
                }
                if(tokens[1].toLowerCase( Locale.ROOT ).equals( "info" )){
                    return new DisplayQuery(tokens[2].replace( ";", "" ));
                }
                System.out.println("Error in display format.");
                break;
            default:
                System.out.println("Usage...");
                break;
        }

        return null;
    }

    //SELECT * FROM <table>;
    public static SelectQuery ParseSelect(String input){
        String[] tokens = input.split( " " );
        if(tokens.length < 4){
            System.out.println("Error in command: not long enough.");
            return null;
        }
        if(!tokens[2].toLowerCase( Locale.ROOT ).equals( "from" )){
            System.out.println("Error in command: Need to use From");
            return null;
        }
        if(!tokens[3].endsWith( ";" )){
            System.out.println("Error: Missing semicolon.");
            return null;
        }

        return new SelectQuery( tokens[1],tokens[3].replace( ";", "" ) );
    }

    //INSERT INTO <table> values <tuple>;
    public static InsertQuery ParseInsert(String input){

        return null;
    }

    //CREATE TABLE <name> (<attr_name1> <attr_type1> primarykey,
    //      <attr_name2> <attr_type2>, <attr_nameN> <attr_typeN>);
    public static CreateQuery ParseCreate(String input){

        return null;
    }



    public static Query CommandParse(String input){
        Query result = new Query();
        String[] temp = input.split(" ", 2);

        String command = temp[0].toLowerCase( Locale.ROOT );

        switch (command){
            case "select":
                result = ParseSelect( input );
                break;
            case "insert":
                result = ParseInsert( input );
                break;
            case "create":
                result = ParseCreate( input );
                break;
            case "display":
                result = ParseDisplay( input );
                break;
            default:
                System.out.println("Error, command not recognized. Recieved: " + command);
                break;
        }

        return result;
    }
}