/**
 * This file handles the parsing of different SQL queries
 * @author Duncan Small
 */
import java.util.HashMap;
import java.util.Locale;

class QueryParser{
    public QueryParser(){

    }

    //display info <table>;
    //display schema;
    public DisplayQuery ParseDisplay(String input){
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
    public SelectQuery ParseSelect(String input){
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
    public InsertQuery ParseInsert(String input){

        return null;
    }

    //CREATE TABLE <name> (<attr_name1> <attr_type1> primarykey,
    //      <attr_name2> <attr_type2>, <attr_nameN> <attr_typeN>);
    public CreateQuery ParseCreate(String input){
        String[] chunks = input.split( "[(]", 2 );
        if(chunks.length < 2){
            System.out.println("Error in formatting.");
            return null;
        }

        String[] keywords = chunks[0].split( " " );

        if(keywords.length < 3){
            System.out.println("Missing arguments for CREATE command.");
            return null;
        }
        if(!keywords[1].toLowerCase( Locale.ROOT ).equals( "table" )){
            System.out.println("Improper use of CREATE command.");
            return null;
        }

        String[] attributes = chunks[1].split( "," );

        if(attributes[attributes.length - 1].endsWith( ");" )) {
            attributes[attributes.length - 1] = attributes[attributes.length - 1].replace( ");", "" );
        } else{
            System.out.println("Error: Formatting issue");
            return null;
        }

        HashMap<String,String> columns = new HashMap<>();
        String pk = null;

        for ( String attr : attributes ) {
            attr = attr.strip();
            String[] temp = attr.split( " "  );
            switch (temp.length){
                case 2:
                    //not primary key
                    columns.put( temp[0], temp[1] ); //make the pairing for name of attribute and the type of attr
                    break;
                case 3:
                    //Primary key
                    if(temp[2].equals( "primarykey" )){
                        if(pk == null){
                            pk = temp[0];
                            columns.put( temp[0],temp[1] );
                            break;
                        } else{
                            System.out.println("Primary key already exists, it was named: " + pk);
                            return null;
                        }

                    }else{
                        System.out.println("Error, expected primary key, got: " + temp[2]);
                        return null;
                    }
                default:
                    System.out.println("Please ensure that attributes are in the following format <name> <type>.");
                    return null;


            }
        }
        return new CreateQuery( keywords[2], columns );
    }



    public Query CommandParse(String input){
        String[] temp = input.split(" ", 2);
        String command = temp[0].toLowerCase( Locale.ROOT );

        switch (command){
            case "select":
                return ParseSelect( input );
            case "insert":
                return ParseInsert( input );
            case "create":
                return ParseCreate( input );
            case "display":
                return ParseDisplay( input );
            default:
                System.out.println("Error, command not recognized. Recieved: " + command);
                return null;
        }
    }
}