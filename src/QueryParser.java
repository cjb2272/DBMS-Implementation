package src;

/*
 * This file handles the parsing of different SQL queries
 * @author Duncan Small, Austin Cepalia
 */

import src.ConditionalTreeNodes.*;

import java.util.*;

@SuppressWarnings("rawtypes")
class QueryParser {

    public QueryParser() {
    }

    public int Precedence(String input){
        return switch (input){
            case "=", ">", "<", "<=", ">=", "!=" -> 3;
            case "and" -> 2;
            case "or" -> 1;
            default -> -1;
        };
    }

    public ConditionTree ParseConditional( String input, ArrayList<String> validColumns, ArrayList<String> overlapColumns, ArrayList<String> tableNames ){
        String[] tokens = input.split( " " );
        Deque<ConditionTree> outputQ = new ArrayDeque<>();
        Deque<String> operatorStack = new ArrayDeque<>();

        for(String token : tokens){
            if(token.equals( "" )){
                continue;
            }
            int prece = Precedence( token );
            if( prece == -1){
                //operand
                List<Object> data = TypeCast( token );
                if(token.contains( "." ) && (int) data.get( 0 ) == 0){
                    String[] tokenSplit =  token.split( "\\." );
                    if(!tableNames.contains( tokenSplit[0] )){
                        System.out.println("Table you reference is not used in this query: " + token);
                        return null;
                    }
                }
                if(validColumns.contains( token ) || validColumns.contains( token.split( "\\." )[token.split( "\\." ).length - 1] )){
                    //attribute
                    if(!token.contains( "." )){
                        if(overlapColumns.contains( token )){
                            System.out.println("Ambiguous column name in conditional: " + token);
                            return null;
                        }
                    }
                    outputQ.add( new AttributeNode( token ) );
                } else{
                    //constant
                    outputQ.add( new ConstantNode( data.get( 1 ), (int) data.get( 0 ) ) );
                }
            } else {
                //operator
                while(!operatorStack.isEmpty() && Precedence( operatorStack.peekLast()) >= prece ){
                    int exitCode = popOffOperatorStack( operatorStack, outputQ );
                    if(exitCode == -1){
                        return null;
                    }
                }
                operatorStack.add( token );
            }
        }
        while(!operatorStack.isEmpty()){
            int exitCode = popOffOperatorStack( operatorStack, outputQ );
            if(exitCode == -1){
                return null;
            }
        }
        return outputQ.pop();
    }

    public int popOffOperatorStack(Deque<String> operatorStack, Deque<ConditionTree> outputQ){
        String current = operatorStack.removeLast();
        ConditionTree R = outputQ.removeLast();
        ConditionTree L = outputQ.removeLast();
        switch (Precedence( current )){
            case 1:
                //or
                if(R.getClass() != OperationNode.class ){
                    System.out.println("Expected OperationNode, got: " + R.getClass()+ ". Token: " + R.getToken());
                    return -1;
                } else if(L.getClass() != OperationNode.class){
                    System.out.println("Expected OperationNode, got: " + L.getClass()+ ". Token: " + L.getToken());
                    return -1;
                } else{
                    outputQ.add( new OrNode( (OperationNode) L, current, (OperationNode) R) );
                    return 1;
                }
            case 2:
                //and
                if(R.getClass() != OperationNode.class ){
                    System.out.println("Expected OperationNode, got: " + R.getClass()+ ". Token: " + R.getToken());
                    return -1;
                } else if(L.getClass() != OperationNode.class){
                    System.out.println("Expected OperationNode, got: " + L.getClass()+ ". Token: " + L.getToken());
                    return -1;
                } else{
                    outputQ.add( new AndNode( (OperationNode) L, current, (OperationNode) R) );
                    return 1;
                }
            case 3:
                //operator
                if(R.getClass() != ConstantNode.class && R.getClass() != AttributeNode.class){
                    System.out.println("Expected ConstantNode or AttributeNode, got: " + R.getClass()+ ". Token: " + R.getToken());
                    return -1;
                } else if(L.getClass() != AttributeNode.class){
                    System.out.println("Expected AttributeNode, got: " + L.getClass()+ ". Token: " + L.getToken());
                    return -1;
                } else{
                    outputQ.add( new OperationNode( (AttributeNode) L, current, (ValueNode) R) );
                    return 1;
                }
            default:
                System.out.println("Expected either an OR, an AND, or an Operator, received: " + current);
                return -1;
        }
    }

    //update <name>
    //set <column_1> = <value>
    //where <condition>;
    public UpdateQuery ParseUpdate(String input){
        String[] setSplit = input.split( "set" );
        if(setSplit.length < 2){
            System.out.println("Missing SET keyword.");
            return null;
        }
        String tableName = setSplit[0].replace( "update", "" ).trim();
        setSplit[1].replace( "set", "");

        String[] whereSplit = setSplit[1].split( "where" );
        String[] equalSplit = whereSplit[0].split( "=" );
        if(equalSplit.length < 2){
            System.out.println("Missing equals sign after SET");
            return null;
        }
        String colName = equalSplit[0].trim();
        List<Object> data = TypeCast( equalSplit[1].trim() );
        ConditionTree where = null;
        if(whereSplit.length != 1){
            //There is a where
            whereSplit[1].replace( "where", "" );
            //parse conditional
            where = ParseConditional( whereSplit[1], Catalog.instance.getAttributeNames( tableName ), new ArrayList<>(), new ArrayList<>(Arrays.asList( tableName )) );
            if(where == null){
                System.out.println("Could not parse conditional statement.");
                return null;
            }
        }
        LinkedHashMap<String, ArrayList<String>> tableColumnDict = new LinkedHashMap<>();
        tableColumnDict.put( tableName, new ArrayList<>(Arrays.asList( colName )) );
        return new UpdateQuery(tableName, colName, tableColumnDict, data, where);
    }

    public DeleteQuery ParseDelete(String input){
        String[] keywords = input.split( "from" );
        if(keywords.length < 2){
            System.out.println("Missing FROM keyword.");
            return null;
        }
        String[] chunks = keywords[1].split( "where" );
        ConditionTree where = null;
        String tableName = chunks[0].replace( "from", "" ).trim();
        if(chunks.length != 1){
            //Where is present so chunks[0] is table name, chunks[1] is conditional
            where = ParseConditional(chunks[1].replace( "where", "" ), Catalog.instance.getAttributeNames(tableName), new ArrayList<>(), new ArrayList<>(Arrays.asList( tableName )) );
            if(where == null){
                System.out.println("Could not parse conditional statement");
                return null;
            }
        }
        ArrayList<String> colNames = Catalog.instance.getAttributeNames(tableName);
        LinkedHashMap<String, ArrayList<String>> tableColumnDict = new LinkedHashMap<>();
        tableColumnDict.put( tableName, colNames );
        return new DeleteQuery(tableName, where, tableColumnDict);
    }

    // display info <table>;
    // display schema;

    /**
     * This function Parses a display command into a DisplayQuery.
     * 
     * @param input The command being parsed
     * @return One of two types of DisplayQuery (table or schema) or null if there
     *         is an error
     */
    public DisplayQuery ParseDisplay(String input) {
        String[] tokens = input.split(" ");

        switch (tokens.length) {
            case 2 -> {

                if (tokens[1].toLowerCase(Locale.ROOT).equals("schema")) {
                    return new DisplayQuery();
                }
                System.out.println("Expected 'schema' got: " + tokens[1]);
            }
            case 3 -> {

                if (tokens[1].toLowerCase(Locale.ROOT).equals("info")) {
                    return new DisplayQuery(tokens[2]);
                }
                System.out.println("Expected 'info' got: " + tokens[1]);
            }
            default -> System.out.println("Must use either 'display info <table>;' or \n 'display schema;' command");
        }
        return null;
    }

    //alter table <name> drop <a_name>;
    //alter table <name> add <a_name> <a_type>;
    //alter table <name> add <a_name> <a_type> default <value>

    /**
     * Takes in the query string for alter.
     * Handles:
     * alter table <name> drop <a_name>;
     * alter table <name> add <a_name> <a_type>;
     * alter table <name> add <a_name> <a_type> default <value>
     * @param input :query string
     * @return Either Null is failed or AlterQuery if successful.
     */
    public Query ParseAlter( String input ) {
        String[] tokens = input.split( " " , 8);


        if(tokens.length < 5){
            System.out.println( "Expected 'alter table <name> drop <a_name>;' or " +
                    "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
            return null;
        }
        else if(!tokens[1].equalsIgnoreCase( "table" )){
            System.out.println("Missing Table keyword.");
            return null;
        }

        String tableName = tokens[2].trim();

        int tableID = Catalog.instance.getTableIdByName(tableName);
        if(tableID == -1){
            System.out.println("Table " + tableName + " does not exist.");
            return null;
        }

        if (tokens.length == 5){
            //drop
            if(!tokens[3].equalsIgnoreCase( "drop" )){
                System.out.println("Expected drop command, got: " + tokens[3]);
                System.out.println( "Use the format 'alter table <name> drop <a_name>;' or " +
                        "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
                return null;
            }

            String colName = tokens[4].trim();

            if(!Catalog.instance.getAttributeNames( tableName ).contains( colName )){
                System.out.println("Column " + colName + " does not exist in table " + tableName + ".");
                return null;
            }

            return new AlterQuery(tableName, tokens[4]);
        }
        else if (tokens.length == 6){
            //alter no default
            if(!tokens[3].equalsIgnoreCase( "add" )){
                System.out.println("Expected add command, got: " + tokens[3]);
                System.out.println( "Use the format 'alter table <name> drop <a_name>;' or " +
                        "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
                return null;
            }
            else if(StringToCode( tokens[5] ) == -1){
                System.out.println("Must use a valid column type, got: " + tokens[5]);
                return null;
            }
            int type = StringToCode( tokens[5] );
            int size;
            if (type == 4 || type == 5) {
                size = GetLength(tokens[5]);
            } else if (type == 3){
                size = Character.BYTES;
            } else if (type == 2) {
                size = Double.BYTES;
            } else {
                size = Integer.BYTES;
            }

            return new AlterQuery( tableName, tokens[4], type, size);

        }
        else if (tokens.length == 8) {
            //alter with default
            int typeCode = StringToCode( tokens[5] );
            if(!tokens[3].equalsIgnoreCase( "add" )){
                System.out.println("Expected add command, got: " + tokens[3]);
                System.out.println( "Use the format 'alter table <name> drop <a_name>;' or " +
                        "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
                return null;
            }
            else if(typeCode == -1){
                System.out.println("Must use a valid column type, got: " + tokens[5]);
                return null;
            } else if (!tokens[6].equalsIgnoreCase("default")) {
                System.out.println( "Use the format 'alter table <name> drop <a_name>;' or " +
                        "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
                return null;
            }

            List typeInfo = TypeCast( tokens[7] );
            String defaultStr = tokens[7];
            int defaultType = (int) typeInfo.get( 0 );
            //Mates sure string type matches and string length is correct
            if((defaultType == 0 && (typeCode == 4 || typeCode == 5))){
                defaultStr = (String) typeInfo.get( 1 );
                defaultStr = defaultStr.substring(1, defaultStr.length() - 1);
                int strLen = GetLength( tokens[5] );
                if(strLen != defaultStr.length() && typeCode == 4){
                    System.out.println("Default Value must follow type constraint: " + tokens[5]);
                    return null;
                }
                else if(defaultStr.length() > strLen){
                    System.out.println("Default Value must follow type constraint: " + tokens[5]);
                    return null;
                }
            }
            //Makes sure type matches
            else if (defaultType != typeCode){
                System.out.println("Default value type must match column type given.");
                System.out.println("Got (" + CodeToString( defaultType ) + ") Expected (" + CodeToString( typeCode ) + ").");
                return null;
            }

            //sets size
            int size;
            if (typeCode == 4 || typeCode == 5) {
                size = GetLength(tokens[5]);
            } else if (typeCode == 3){
                size = Character.BYTES;
            } else if (typeCode == 2) {
                size = Double.BYTES;
            } else {
                size = Integer.BYTES;
            }

            return new AlterQuery( tableName, tokens[4], typeCode,size, defaultStr);
        }
        else{
            System.out.println( "Expected 'alter table <name> drop <a_name>;' or " +
                    "'alter table <name> add <a_name> <a_type> [optional: default <value>];'" );
            return null;
        }
    }

    /**
     * Processes a drop query.
     * Handles:
     * drop table <name>
     * @param input : drop query
     * @return DropQuery if successful and null if unsuccessful.
     */
    public DropQuery ParseDrop(String input) {
        String[] tokens = input.split(" ");

        if (tokens.length == 3) {
            if (tokens[1].toLowerCase(Locale.ROOT).equals("table")) {
                String tableName = tokens[2];
                return new DropQuery(tableName);
            }
            else {
                System.out.println("Expected 'table' got: " + tokens[1]);
                return null;
            }
        }
        else {
            System.out.println("Expected 'drop table <name>'");
            return null;
        }
    }


    /**
     * This function parses a Select command into a SelectQuery Object
     * 
     * @param input The Select command being parsed
     * @return A SelectQuery object representing the command that was passed or null
     *         if there is an error
     */
    public SelectQuery ParseSelect(String input) {
        if ( !input.contains( "from" ) ) {
            System.out.println( "Missing FROM keyword." );
            return null;
        }
        String[] tokens = input.split( "select|from|where|orderby" );
        if ( tokens.length < 3 ) {
            System.out.println( "Expected 'SELECT * FROM <table>' format." );
            return null;
        }
        int stepCounter = 0;
        ArrayList<String> colNames = null;
        ArrayList<String> tableNames = null;
        ConditionTree where = null;
        Boolean starFlag = false;
        String conditional = "";
        String orderBy = "";
        for ( String s : tokens ) {
            if ( s.equals( "" ) && stepCounter == 0 ) {
                stepCounter++;
                continue;
            }
            if ( stepCounter == 1 ) {
                if(tokens[stepCounter].trim().equals( "*" )){
                    starFlag = true;
                    stepCounter++;
                    continue;
                }
                colNames = new ArrayList<>( Arrays.asList( tokens[stepCounter].split( "," ) ) );
                for ( int i = 0; i < colNames.size(); i++ ) {
                    colNames.set( i, colNames.get( i ).trim() );
                }
                stepCounter++;
                continue;
            }
            if (stepCounter == 2){
                tableNames = new ArrayList<>( Arrays.asList( tokens[stepCounter].split( "," ) ) );
                for ( int i = 0; i < tableNames.size(); i++ ) {
                    tableNames.set( i, tableNames.get( i ).trim() );
                }
                stepCounter++;
                continue;
            }
            if (stepCounter == 3){
                conditional = tokens[stepCounter];
                stepCounter++;
                continue;
            }
            if(stepCounter == 4){
                orderBy = tokens[stepCounter].trim();
                stepCounter++;
                continue;
            }
        }


        ArrayList<String> colCheckList = new ArrayList<>();
        if(!starFlag) {
            colCheckList = new ArrayList<>( colNames ); //make a copy to ensure each col exists
        }
        LinkedHashMap<String, ArrayList<String>> tableColumnDict = new LinkedHashMap<>();

        //only for conditionals
        ArrayList<String> validColumnNames = new ArrayList<>();

        //makes sure that tables are valid
        for(String t : tableNames){
            int tableID = Catalog.instance.getTableIdByName( t );
            if(tableID == -1){
                System.out.println("Table '" + t + "' does not exist.");
                return null;
            } else {
                ArrayList<String> col = Catalog.instance.getAttributeNames( t );
                validColumnNames.addAll( col );
            }
        }

        ArrayList<String> overlapColumns = new ArrayList<>();

        //looks for overlap in table columns
        for(String t1 : tableNames){
            for(String t2 : tableNames){
                if(t1.equals( t2 )){
                    //skip
                } else{
                    ArrayList<String> t1Col = Catalog.instance.getAttributeNames( t1 );
                    ArrayList<String> t2Col = Catalog.instance.getAttributeNames( t2 );
                    for(String col1 : t1Col){
                        if( t2Col.contains(col1) ){
                            overlapColumns.add( col1 );
                        }
                    }
                }
            }
        }

        //makes sure column names aren't ambiguous and exist
        if(!starFlag) {
            for ( String c : colNames ) {
                if ( overlapColumns.contains( c ) ) {
                    System.out.println( "Ambiguous column: " + c );
                    return null;
                }
                if ( c.contains( "." ) ) {
                    String[] cSplit = c.split( "\\." );
                    if ( !Catalog.instance.getAttributeNames( cSplit[0] ).contains( cSplit[1] ) ) {
                        System.out.println( "Table [" + cSplit[0] + "] does not have column [" + cSplit[1] + "]." );
                        return null;
                    } else if (!tableNames.contains( cSplit[0] )){
                        System.out.println("Table referenced in select not included in query: " +  c);
                        return null;
                    }
                } else{
                    if(!validColumnNames.contains( c )){
                        if(c.equals( "*" )){
                            System.out.println("Cannot have multiple columns and * in the same query.");
                            return null;
                        }
                        System.out.println("Column does not exist: " + c);
                        return null;
                    }
                }
                colCheckList.remove( c );
            }
        }

        boolean orderByTable = false;
        ArrayList<String> orderByInfo = new ArrayList<>();
        if(!orderBy.equals( "" )) {
            if ( orderBy.contains( "." ) ) {
                orderByTable = true;
                String[] orderBySplit = orderBy.split( "\\." );
                if ( Catalog.instance.getTableIdByName( orderBySplit[0] ) == -1 ) {
                    System.out.println( "Table " + orderBySplit[0] + " used in orderBy does not exist." );
                    return null;
                } else {
                    if ( !Catalog.instance.getAttributeNames( orderBySplit[0] ).contains( orderBySplit[1] ) ) {
                        System.out.println( "Column " + orderBySplit[1] + " does not exist in table " + orderBySplit[0] + "." );
                        return null;
                    }
                }
            } else {
                if ( overlapColumns.contains( orderBy ) ) {
                    System.out.println( "OrderBy column " + orderBy + " is ambiguous." );
                    return null;
                } else {
                    if ( !validColumnNames.contains( orderBy ) ) {
                        System.out.println( "Column " + orderBy + " could not be found." );
                        return null;
                    }
                }
            }
            if ( !starFlag && !colNames.contains( orderBy ) ) {
                System.out.println( "Must include attribute in select to use OrderBy." );
                return null;
            }

            if ( orderByTable ) {
                String[] orderBySplit = orderBy.split( "\\." );
                orderByInfo.add( orderBySplit[0] );
                orderByInfo.add( orderBySplit[1] );
            } else {
                for ( String t : tableNames ) {
                    if ( Catalog.instance.getAttributeNames( t ).contains( orderBy ) ) {
                        orderByInfo.add( t );
                        orderByInfo.add( orderBy );
                        break;
                    }
                }
                if ( orderByInfo.size() == 0 ) {
                    System.out.println( "Error trying to find which table " + orderBy + " belongs to." );
                    return null;
                }
            }
        }

        //Building TableColumnDictionary since every column is valid
        for(String t : tableNames){
            ArrayList<String> col = Catalog.instance.getAttributeNames( t );
            ArrayList<String> colFromThisTable = new ArrayList<>();
            if(starFlag){
                colFromThisTable.addAll( col );
            } else {
                for ( String c : colNames ) {
                    if ( c.contains( "." ) ) {
                        String[] cSplit = c.split( "\\." );
                        if ( cSplit[0].equals( t ) ) {
                            colFromThisTable.add( cSplit[1] );
                        }
                    } else {
                        if ( col.contains( c ) ) {
                            colFromThisTable.add( c );
                        }
                    }
                }
            }
            tableColumnDict.put( t, colFromThisTable );
        }

        if(!starFlag && colCheckList.size() > 0){
            StringBuilder res = new StringBuilder( "[" );
            for(String col: colCheckList){
                res.append( " " ).append( col );
            }
            res.append( " ]" );
            System.out.println("The following columns could not be found: " + res);
            return null;
        }

        if(!conditional.equals( "" )){
            where = ParseConditional( conditional, validColumnNames, overlapColumns, tableNames );
            if(where == null){
                System.out.println("Could not parse conditional statement.");
                return null;
            }
        }

        return new SelectQuery( tableColumnDict, where, orderByInfo, starFlag );
    }

    // INSERT INTO <table> values <tuple>;
    // insert into foo values (1 "foo bar" true 2.1),
    // (3"baz" true 4.14),
    // (2"bar" false 5.2),
    // (5 "true" true null);

    /**
     * This function takes a given Insert Command and parses it into an InsertQuery
     * object
     * 
     * @param input The command to be parsed
     * @return An InsertQuery object representing the command, or null if there is
     *         an error
     */
    public InsertQuery ParseInsert(String input) {
        String[] separate = input.split("values");
        if (separate.length != 2) {
            System.out.println("Missing VALUES keyword.");
            return null;
        }

        String[] keywords = separate[0].split(" ");
        if (keywords.length != 3) {
            System.out.println("Error in formatting: " + separate[0]);
            return null;
        }

        if (!keywords[1].toLowerCase(Locale.ROOT).equals("into")) {
            System.out.println("Missing into keyword.");
            return null;
        }

        String tableName = keywords[2];

        if (Catalog.instance.getTableSchemaByName(tableName) == null) {
            System.out.println("No such table " + tableName);
            return null;
        }

        ArrayList<Integer> tableAttrList = Catalog.instance.getTableAttributeTypesByName(tableName);
        ArrayList<ArrayList<Object>> formattedTuples = new ArrayList<>();

        String[] tuples = separate[1].split(",");
        for (String s : tuples) {
            s = s.replaceAll("[();]", "");
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<Integer> dataTypes = new ArrayList<>();
            if (s.contains("\"")) {
                // Strings in values, so there might be spaces
                String remaining = s;

                boolean open = true;
                while (remaining.contains("\"")) {
                    String[] temp = remaining.split("\"", 2);
                    remaining = temp.length == 2 ? temp[1] : "";
                    open = !open;
                    if (open) {
                        // whole chunk is string
                        values.add(temp[0]);
                        dataTypes.add(0);
                        dataTypes.add(temp[0].length());
                    } else {
                        // Chunk is other values
                        String[] nonStringVals = temp[0].split(" ");
                        for (String val : nonStringVals) {
                            if (val.equals(" ") || val.equals("")) {
                                continue;
                            }
                            List types = TypeCast(val);
                            values.add(types.get(1));
                            dataTypes.add((int) types.get(0));
                        }
                    }
                }
                for (String val : remaining.split(" ")) {
                    if (val.equals("")) {
                        continue;
                    }
                    List types = TypeCast(val);
                    values.add(types.get(1));
                    dataTypes.add((int) types.get(0));
                }
            } else {
                // No Strings in values
                for (String val : s.split(" ")) {
                    if (val.equals("")) {
                        continue;
                    }
                    List types = TypeCast(val);
                    values.add(types.get(1));
                    dataTypes.add((int) types.get(0));
                }
            }

            if (AttributeMatch(tableAttrList, dataTypes)) {
                formattedTuples.add(values);
            } else {
                if (formattedTuples.size() == 0) {
                    return null;
                }

                System.out.println("Only the items before (" + s.strip() + ") will be inserted");
                return new InsertQuery(tableName, formattedTuples);
            }
        }

        return new InsertQuery(tableName, formattedTuples);
    }

    /**
     * This function parses a create table command into a CreateQuery object
     *
     * @param input The command being parsed
     * @return a CreateQuery representing the command given, null if there is an
     *         error
     */
    public CreateQuery ParseCreate(String input) {
        if (!input.endsWith(")")) {
            System.out.println( """
                    Missing closing Parenthesis. Must use this format:
                    CREATE TABLE <name> (<attr_name1> <attr_type1> primarykey,
                        <attr_name2> <attr_type2>, <attr_nameN> <attr_typeN>);""" );
            return null;
        }

        String[] chunks = input.split("[(]", 2);
        if (chunks.length < 2) {
            System.out.println( """
                    Missing opening Parenthesis. Must use this format:
                    CREATE TABLE <name> (<attr_name1> <attr_type1> primarykey,
                        <attr_name2> <attr_type2>, <attr_nameN> <attr_typeN>);""" );
            return null;
        }

        String[] keywords = chunks[0].split(" ");

        if (keywords.length < 3) {
            System.out.println("Missing arguments for CREATE command.");
            return null;
        }
        if (!keywords[1].equalsIgnoreCase("table")) {
            System.out.println("Missing TABLE keyword.");
            return null;
        }

        String[] attributes = chunks[1].split(",");

        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<Integer> dataTypes = new ArrayList<>();
        ArrayList<Integer> varLengthSizes = new ArrayList<>();
        ArrayList<Integer> constraints = new ArrayList<>();
        String pk = null;

        for (String attr : attributes) {
            attr = attr.strip();
            String[] temp = attr.split(" ");

            int constraintCode = 0;

            if (temp.length == 1) {

                if (dataTypes.size() == 0 && temp[0].equals(")")) {
                    System.out.println("Missing attributes from CREATE command.");
                } else {
                    System.out.println("Too few arguments for attribute. Got: " + attr);
                }
                return null;
            } else if (temp.length == 3) {
                // Primary key
                String constraint = temp[2].replace(")", "");
                if (constraint.equalsIgnoreCase("primarykey")) {
                    constraintCode = 3;
                    if (pk == null) {
                        pk = temp[0];
                    } else {
                        System.out.println("Primary key already exists, it was named: " + pk);
                        return null;
                    }
                } else if(constraint.equalsIgnoreCase( "notnull" )){
                    constraintCode = 2;
                } else if(constraint.equalsIgnoreCase( "unique" )){
                    constraintCode = 1;
                } else {
                    System.out.println("Too many arguments: Expected constraint, got: " + temp[2]);
                    return null;
                }
            } else if (temp.length == 4){
                temp[3] = temp[3].replace(")", "");
                if((temp[2].equalsIgnoreCase( "unique" ) && temp[3].equalsIgnoreCase( "notnull" )) || (temp[3].equalsIgnoreCase( "unique" ) && temp[2].equalsIgnoreCase( "notnull" ))){
                    constraintCode = 3;
                } else{
                    System.out.println("Too many arguments: Expected constraints, got: " + temp[2] + " and " + temp[3]);
                    return null;
                }
            } else if (temp.length > 4) {
                //TODO fix usage method
                System.out.println( """
                        Please ensure that attributes are in the following format:
                        <name> <type>     -or-
                        <name> <type> primarykey""" );
                return null;
            }

            String name = temp[0];
            if (columnNames.contains(name)) {
                System.out.println("Duplicate attribute name \"" + name + "\"");
                return null;
            }

            int result = StringToCode(temp[1]);
            if (result == -1) {
                System.out.println(
                        "You must give one of the accepted types for an attribute:\n Integer, Double, Boolean, Char(x), or Varchar(x).");
                System.out.println("Received: " + temp[1]);
                return null;
            }

            columnNames.add(name);
            dataTypes.add(result);

            if (result == 4 || result == 5) {
                int charLength = GetLength(temp[1]);
                varLengthSizes.add(charLength);
                dataTypes.add(charLength);
            } else {
                varLengthSizes.add(-1);
            }

            constraints.add( constraintCode );

        }

        if (pk == null) {
            System.out.println("No primary key defined.");
            return null;
        }

        return new CreateQuery(keywords[2], columnNames, dataTypes, varLengthSizes, pk, constraints);
    }

    /**
     * This function compares two lists of data types to ensure that data being
     * inserted into a table matches the columns
     * 
     * @param tableAttrList The list of data types belonging to the table
     * @param dataAttrList  The list of data types belonging to the data being
     *                      inserted
     * @return True if the data being inserted is a match, False if there are
     *         discrepancy's
     */
    public boolean AttributeMatch(ArrayList<Integer> tableAttrList, ArrayList<Integer> dataAttrList) {
        if (tableAttrList.size() != dataAttrList.size() && !dataAttrList.contains( 6 )) {
            System.out.println("Error! Table and Data Attribute list do not match with length!");
            return false;
        }

        Integer code = -1;
        Integer expected = -1;

        //offset for when a string is expected; skips over the expected char length
        int j = 0;

        for (int i = 0; i < dataAttrList.size(); i++) {
            if(dataAttrList.get( i ) == 6){
                if(tableAttrList.get( i + j) == 4 || tableAttrList.get( i + j) == 6){
                    j++;
                }
                continue;
            }
            else if (dataAttrList.get(i) == 0 && tableAttrList.get(i + j) == 4) {
                try {
                    if (!dataAttrList.get(i + 1).equals(tableAttrList.get(i + j + 1))) {
                        System.out.println("Error! Got Char with length " + dataAttrList.get(i + 1).toString()
                                + ", expected Char with length " + tableAttrList.get(i + j + 1).toString());
                        return false;
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Error! Expected Char length in data type array.");
                    return false;
                }
                i++;
                continue;
            } else if (dataAttrList.get(i) == 0 && tableAttrList.get(i + j) == 5) {
                try {
                    if (dataAttrList.get(i + 1) > tableAttrList.get(i + j + 1)) {
                        System.out.println("Error! Got VarChar with length " + dataAttrList.get(i + 1).toString()
                                + ", expected Char with length up to" + tableAttrList.get(i + j + 1).toString());
                        return false;
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Error! Expected VarChar length in data type array.");
                    return false;
                }
                i++;
                continue;
            }
            if (!dataAttrList.get(i).equals(tableAttrList.get(i + j))) {
                code = dataAttrList.get(i);
                expected = tableAttrList.get(i + j);
                break;

            }
        }

        if (code == -1 && expected == -1) {
            return true;
        }

        System.out.println(
                "Invalid data types: expected (" + CodeToString(expected) + ") got (" + CodeToString(code) + ")");
        return false;
    }

    /**
     * Converts the code of a data type to the Data type string that it represents
     * 
     * @param code The code that represents a given data type
     * @return A String that corresponds to the data type code
     */
    public static String CodeToString(Integer code) {
        return switch (code) {
            case 0 -> "String";
            case 1 -> "Integer";
            case 2 -> "Double";
            case 3 -> "Boolean";
            case 4 -> "Char(x)";
            case 5 -> "VarChar(x)";
            case 6 -> "null";
            default -> "Data-type not recognized";
        };
    }

    /**
     * Converts a string containing a data type into a code that represents it
     * 
     * @param str The Data type being converted
     * @return The Integer that represents the given data type
     */
    public Integer StringToCode(String str) {
        String[] temp = str.split("[()]");

        return switch (temp[0].toLowerCase(Locale.ROOT)) {
            case "integer" -> 1;
            case "double" -> 2;
            case "boolean" -> 3;
            case "char" -> 4;
            case "varchar" -> 5;
            case "null" -> 6;
            default -> -1;
        };
    }

    /**
     * This just grabs a number that is inside parenthesis, used to get the x from
     * "Char(x)"
     * 
     * @param str The string of "VarChar(x)" or "Char(x)"
     * @return the integer that is "x" ^^^^
     */
    public Integer GetLength(String str) {
        String temp = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
        return Integer.parseInt(temp);
    }

    /**
     * This function gets the size in bytes of a given data type
     * 
     * @param type The integer representing a given data type
     * @return The number of bytes that this data type will take up
     */
    public static int getDataTypeSize(int type) {

        // This does not handle varchar. That has to be calculated by the caller

        return switch (type) {
            case 1 -> Integer.SIZE;
            case 2 -> Double.SIZE;
            case 3 -> 1; // I know we're not supposed to hardcode these, but Boolean.SIZE doesn't exist
                         // and my research didn't return an alternative way of getting bool size -AC
            case 4 -> Character.SIZE;
            default -> -1;
        };
    }

    // CREATE TABLE <name> (<attr_name1> <attr_type1> primarykey,
    // <attr_name2> <attr_type2>, <attr_nameN> <attr_typeN>);



    /**
     * This function takes a given piece of data as a string, like an integer
     * boolean etc., and turns
     * it into the correct object by method of trial and error
     * 
     * @param input The piece of data being casted into a type
     * @return a tuple of two pieces of info. First will be the integer representing
     *         which data type
     *         the data was turned into and Second will be the data itself as the
     *         correct type
     */
    public List<Object> TypeCast(String input) {
        try {
            int i = Integer.parseInt(input);
            return Arrays.asList(1, i);
        } catch (NumberFormatException e) {
            try {
                double d = Double.parseDouble(input);
                return Arrays.asList(2, d);
            } catch (NumberFormatException e1) {
                if (input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
                    return Arrays.asList(3, Boolean.parseBoolean(input));
                }
                else if(input.equals("null")){
                    return Arrays.asList( 6, null );
                }
                else {
                    return Arrays.asList(0, input);
                }
            }
        }
    }

    /**
     * This just takes a given command passed from standard in and takes the first
     * word
     * then points it in the correct direction.
     * 
     * @param input The command that was given, which will be parsed by the
     *              corresponding function
     * @return Some kind of Query if it was successfully parsed, or null if there
     *         was an error
     */
    public Query CommandParse(String input) {
        if (!input.endsWith("; ") && !input.endsWith(";")) {
            System.out.println("Missing Semicolon.");
            return null;
        }

        input = input.replace(";", "").trim();

        String[] temp = input.split(" ", 2);
        String command = temp[0].toLowerCase(Locale.ROOT);

        switch (command) {
            case "select":
                return ParseSelect(input);
            case "insert":
                return ParseInsert(input);
            case "create":
                return ParseCreate(input);
            case "display":
                return ParseDisplay(input);
            case "drop":
                return ParseDrop(input);
            case "alter":
                return ParseAlter(input);
            case "update":
                return ParseUpdate(input);
            case "delete":
                return ParseDelete(input);
            default:
                System.out.println("Error, command not recognized. Received: " + command);
                return null;
        }
    }
}