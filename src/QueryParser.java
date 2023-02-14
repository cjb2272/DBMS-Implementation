package src;

/*
 * This file handles the parsing of different SQL queries
 * @author Duncan Small
 */

import java.util.*;

class QueryParser{

    private final SchemaManager schemaManager;
    private final StorageManager storageManager;

    public QueryParser(StorageManager storageManager, SchemaManager schemaManager){
        this.storageManager = storageManager;
        this.schemaManager = schemaManager;
    }

    //display info <table>;
    //display schema;
    public DisplayQuery ParseDisplay(String input){
        String[] tokens = input.split( " " );

        switch (tokens.length) {
            case 2 -> {
                if ( !tokens[1].endsWith( ";" ) ) {
                    System.out.println( "Missing Semicolon" );
                    return null;
                }
                if ( tokens[1].toLowerCase( Locale.ROOT ).equals( "schema;" ) ) {
                    return new DisplayQuery( storageManager, schemaManager );
                }
                System.out.println( "Error in display format." );
            }
            case 3 -> {
                if ( !tokens[2].endsWith( ";" ) ) {
                    System.out.println( "Missing Semicolon." );
                    return null;
                }
                if ( tokens[1].toLowerCase( Locale.ROOT ).equals( "info" ) ) {
                    return new DisplayQuery( storageManager, schemaManager, tokens[2].replace( ";", "" ) );
                }
                System.out.println( "Error in display format." );
            }
            default -> System.out.println( "Usage..." );
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

        return new SelectQuery( storageManager, schemaManager, tokens[1],tokens[3].replace( ";", "" ) );
    }

    //INSERT INTO <table> values <tuple>;
    //insert into foo values (1 "foo bar" true 2.1),
    //(3"baz" true 4.14),
    //(2"bar" false 5.2),
    //(5 "true" true null);
    public InsertQuery ParseInsert(String input){
        String[] separate = input.split( "values" );
        if(separate.length != 2 ){
            System.out.println("Missing values keyword.");
            return null;
        }

        String[] keywords = separate[0].split( " "  );
        if(keywords.length != 3){
            System.out.println("Error in formatting: " + separate[0]);
            return null;
        }

        if(!keywords[1].toLowerCase( Locale.ROOT ).equals( "into" )){
            System.out.println("Missing into keyword.");
            return null;
        }

        String tableName = keywords[2];

        //ArrayList<Integer> tableAttrList = schemaManager.getAttrList(tableName);
        ArrayList<Integer> tableAttrList = new ArrayList<>();

        String[] tuples = separate[1].split( "," );
        ArrayList<ArrayList<Object>> formattedTuples = new ArrayList<>();

        for(String s : tuples){
            s = s.replaceAll( "[();]","" );
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<String> strings = new ArrayList<>();
            ArrayList<Integer> dataTypes = new ArrayList<>();
            if(s.contains( "\"" )){
                //Strings in values, so there might be spaces
                String remaining = s;

                Boolean open = true;
                while(remaining.contains( "\"" )){
                    String[] temp = remaining.split( "\"", 2 );
                    remaining = temp.length == 2 ? temp[1] : "";
                    open = !open;
                    if(open){
                        //whole chunk is string
                        values.add( temp[0] );
                        dataTypes.add( 0 );
                        dataTypes.add( temp[0].length() );
                    } else{
                        //Chunk is other values
                        String[] nonStringVals = temp[0].split( " " );
                        for(String val : nonStringVals){
                            if(val.equals( " " ) || val.equals( "" )) {
                                continue;
                            }
                            List types = TypeCast( val );
                            values.add( types.get( 1 ) );
                            dataTypes.add( (int) types.get( 0 ) );
                        }
                    }
                }
                for(String val : remaining.split( " " )){
                    if(val.equals( "" )) {
                        continue;
                    }
                    List types = TypeCast( val );
                    values.add( types.get( 1 ) );
                    dataTypes.add( (int) types.get( 0 ) );
                }
            } else{
                //No Strings in values
                for(String val : s.split( " " )){
                    if(val.equals( "" )) {
                        continue;
                    }
                    List types = TypeCast( val );
                    values.add( types.get( 1 ) );
                    dataTypes.add( (int) types.get( 0 ) );
                }
            }

            if(AttributeMatch( tableAttrList, dataTypes )){
                formattedTuples.add( values );
            } else{
                System.out.println("The following values were not inserted: ");
                System.out.println(values);
            }
        }

        return new InsertQuery( storageManager, schemaManager, tableName, formattedTuples );
    }

    public boolean AttributeMatch(ArrayList<Integer> tableAttrList, ArrayList<Integer> dataAttrList){
        if(tableAttrList.size() != dataAttrList.size()) {
            System.out.println("Error! Table and Data Attribute list do not match with length!");
            return false;
        }

        Integer code = -1;
        Integer expected = -1;

        for ( int i = 0; i < tableAttrList.size(); i++ ) {
            if(dataAttrList.get( i ) == 0 && (tableAttrList.get( i ) == 4 || tableAttrList.get( i ) == 5)){
                try{
                    if ( !dataAttrList.get( i + 1 ).equals(tableAttrList.get( i + 1 ) )) {
                        System.out.println( "Error! Got Char with length " + dataAttrList.get( i + 1 ).toString() + ", expected Char with length " + tableAttrList.get( i + 1 ).toString() );
                        return false;
                    }
                } catch (ArrayIndexOutOfBoundsException e){
                    System.out.println("Error! Expected Char length in data type array.");
                    return false;
                }
                continue;
            }
            if(!dataAttrList.get( i ).equals(tableAttrList.get( i ))){
                code = dataAttrList.get( i );
                expected = tableAttrList.get( i );
                break;

            }
        }

        if(code == -1 && expected == -1){
            return true;
        }

        System.out.println("Error! Variable entered was " + CodeToString( code ) + ", Expected " + CodeToString( expected ));
        return false;
    }

    public String CodeToString(Integer code){
        return switch (code) {
            case 1 -> "Integer";
            case 2 -> "Double";
            case 3 -> "Boolean";
            case 4 -> "Char(x)";
            case 5 -> "VarChar(x)";
            default -> "Error, code out of bounds";
        };
    }

    public Integer StringToCode(String str){
        String[] temp = str.split( "[(]" );

        return switch (temp[0].toLowerCase( Locale.ROOT )) {
            case "integer" -> 1;
            case "double" -> 2;
            case "boolean" -> 3;
            case "char" -> 4;
            case "varchar" -> 5;
            default -> -1;
        };
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

        if(attributes[attributes.length - 1].endsWith( "); " )) {
            attributes[attributes.length - 1] = attributes[attributes.length - 1].replace( ");", "" );
        } else{
            System.out.println("Error: Formatting issue");
            return null;
        }

        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<Integer> dataTypes = new ArrayList<>();
        String pk = null;

        for ( String attr : attributes ) {
            attr = attr.strip();
            String[] temp = attr.split( " "  );
            switch (temp.length){
                case 2:
                    //not primary key
                    columnNames.add( temp[0]);
                    dataTypes.add( StringToCode( temp[1]) ); //make the pairing for name of attribute and the type of attr
                    break;
                case 3:
                    //Primary key
                    if(temp[2].equals( "primarykey" )){
                        if(pk == null){
                            pk = temp[0];
                            columnNames.add( temp[0]);
                            dataTypes.add( StringToCode( temp[1]) );
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
        return new CreateQuery(storageManager, schemaManager, keywords[2], columnNames, dataTypes, pk );
    }

    public List<Object> TypeCast(String input){
        try {
            int i = Integer.parseInt(input);
            return Arrays.asList(1, i);
        } catch (NumberFormatException e) {
            try {
                double d = Double.parseDouble(input);
                return Arrays.asList( 2, d );
            } catch (NumberFormatException e1) {
                if (input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
                    return Arrays.asList( 3, Boolean.parseBoolean(input) );
                } else {
                    return Arrays.asList( 0, input );
                }
            }
        }
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
                System.out.println("Error, command not recognized. Received: " + command);
                return null;
        }
    }
}