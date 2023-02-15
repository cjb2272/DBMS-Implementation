package src;
/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker, Austin Cepalia
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Storage Manager class responsible for all communication and
 * direct access to hardware
 */
public class StorageManager {

    private String rootPath;

    public StorageManager(String rootPath) {
        this.rootPath = rootPath;
    }

    /*
    Called by the SQL parser to create a new table file on disk.
    The SQL parser should have already verified that the table doesn't already exist (and has valid attributes)
    by asking the schema.
    The Table object is returned as a convenience, but since everything is page-based (and the Table object is never
    directly in the buffer), this isn't strictly needed.
     */
    public Table createTable(int ID, String name, ArrayList<String> columnNames, ArrayList<Integer> dataTypes) {

        // creation of a new table never involves the buffer (there are no pages to start), so the file is created here.
        DataOutputStream output=null;
        try {
            output = new DataOutputStream(new FileOutputStream(rootPath + "\\" + name));

            // no pages in a new table file, so write 0
            output.writeInt(0);

            // write out the ID of this tale

            output.flush();
            output.close();

            return new Table(ID, name, columnNames, dataTypes);


        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally{
            try{
                output.close();
            } catch(Exception e){
                System.err.println(e.getMessage());
            }
        }

        return null;
    }

    /*
    Returns the requested row(s) of data. The Query object calling this is expected to print it out.
     */
    public ArrayList<ArrayList<String>> SelectData() {
        return null; // method stub
    }


    /**
     * The Buffer Manager
     * Has Two Public Methods, GetPage() and PurgeBuffer()
     * The Buffer is in place to ideally reduce read/writes to file system
     *
     * THIS SHOULD BE COMPLETELY INVISIBLE TO THE REST OF THE PROGRAM (besides StorageManager).
     * Everything should go through StorageManager.
     * TODO MUST HANDLE FILE READ AND WRITE ERRORS
     */
    public class BufferManager {


        //will be THE NEXT value to assign when page read
        //will increase indefinitely (long better than int...)
        private long counterForLRU = 0;

        // The Buffer Itself
        ArrayList<Page> PageBuffer= new ArrayList<Page>();

        //should I not be using a getter in any fashion, how should i do this structurally
        //the best way, don't want to be making copies all the time when assigning return
        // of getter to var
        public ArrayList<Page> GetPageBuffer() { return this.PageBuffer; }


        /**
         * Request A page by table number id and page number
         * If page is in buffer, no disk access needed, otherwise,
         * ensure room in buffer, read into buffer, and pass page
         * IOException can probably be removed
         * todo methods that make changes to pages need to update page's
         *  isModified boolean bc not every page read to buffer will
         *  end up being modified...
         *
         * @param tableNumber table num (table is file on disk)
         * @param pageNumber page num
         */
        public Page GetPage(int tableNumber, int pageNumber) throws IOException {
            ArrayList<Page> pageBuffer = GetPageBuffer();
            int maxBufferSize = Main.bufferSizeLimit;
            //if block is present in pageBuffer ArrayList already then return that page
            for (Page inBufferPage : pageBuffer) {
                if (inBufferPage.getPageNumberID() == pageNumber) {
                    inBufferPage.setLruLongValue(counterForLRU); //update count and increment counterForLRU
                    counterForLRU++;
                    //should this method be handling set of IsModified?
                    //hand page off, we do not need to read from
                    //disk since already in buffer
                    return inBufferPage;
                }
            }
            //At beginning of program buffer will not be at capacity,
            //so we call read immediately
            if (pageBuffer.size() < maxBufferSize) {
                Page newlyReadPage = ReadPageFromDisk(tableNumber, pageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                pageBuffer.add(newlyReadPage);
                return newlyReadPage;
            }
            else {
                int indexOfLRU = 0;
                long tempLRUCountVal = Long.MAX_VALUE;
                //find the LRU page
                for (int curPageIndex = 0; curPageIndex <= maxBufferSize; curPageIndex++) {
                    long lruCountVal = pageBuffer.get(curPageIndex).getLruLongValue();
                    if (lruCountVal < tempLRUCountVal) {
                        tempLRUCountVal = lruCountVal;
                        indexOfLRU = curPageIndex;
                    }
                }
                // write the LRU page to hardware
                // (if it has been modified bc if it hasn't copy on disk already same)
                //if (pageBuffer.get(indexOfLRU).getisModified()) { todo uncomment once tested
                    WritePageToDisk(pageBuffer.get(indexOfLRU));
                //}
                //read new page from hardware into buffer
                Page newlyReadPage = ReadPageFromDisk(tableNumber, pageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                pageBuffer.add(indexOfLRU, newlyReadPage); //place page in buffer at location
                return newlyReadPage;                      //we wrote out page
            }
        }

        /**
         * This Method will
         * @param tableNum the table number corresponding to a table/file with needed page
         * @param pageNum the page number identifying the page needed?
         * @return Page including attribute of its structural representation
         * @throws IOException .
         */
        private Page ReadPageFromDisk(int tableNum, int pageNum) throws IOException {
            /* NEED TO KNOW DATA TYPES FOR parsing
                READING and WRITING data to hardware,
               will grab these data types from catalog
               will individual page know the table scheme for which it belongs too?
            */

            //seek through table file to memory you want and read in page size
            //validity of file path and add proper extension todo
            String tableFilePath = rootPath + "tables/" + tableNum + ".";
            RandomAccessFile file = new RandomAccessFile(tableFilePath, "r");

            byte[] pageByteArray = new byte[0];
            ByteBuffer byteBuffer = ByteBuffer.wrap(pageByteArray);

            //probably will not bring to exact location
            file.seek(pageNum * Main.pageSize);
            //Read first portions of page coming before records
            Integer pageNumber = file.readInt(); //first 4 bytes of page are its number
            Integer numRecords = file.readInt(); //second 4 bytes of page is the num records contained
            for (int rcrd = 0; rcrd < numRecords; rcrd++) {
                // LOOP in the order of data types expected - data types cannot be stored in pages,
                // MUST be stored in Catalog ONLY for a given page
                int[] typeIntegers = new int[0];
                //new int[] typeIntegers = SchemaManager.ReadTableSchemaFromCatalogFile(tableNum);
                for (int typeInt : typeIntegers) {
                    switch (typeInt) {
                        case 1: //Integer
                            break;
                        case 2: //Double
                            break;
                        case 3: //Boolean
                            break;
                        case 4: //Char(x) standard string fixed array of len x, padding needs to be removed
                            int x = 0; //need to know x
                            for (int ch = 0; ch < x; ch++) {
                                //tood
                            }
                            //remove padding for Char(x), stand string type
                            break;
                        case 5: //Varchar(x) variable size array of max len x NOT Padded
                            //need to know how many characters are in varchar - set to numChars
                            //loop for that many chars, calling file.readChar()
                            //records with var chars cause scenario of records not being same size
                            int numChars = 0;
                            for (int chr = 0; chr < numChars; chr++) {
                                System.out.println(); //todo
                            }
                    }
                } //END LOOP


            } //END LOOP NUM RECORDS

            Page readPage = Page.parse_bytes(tableNum, pageByteArray);

            return readPage;
        }

        /**
         * The write page to disk method is only called by the buffer manager when
         * a page in the buffer has been modified, and thus the corresponding copy
         * of that page on the disk is outdated.
         * todo would this also be called when page is first created, i believe so
         * Need to ensure beginning of page values coming before records are written
         * properly
         * @param pageToWrite the page being written to hardware
         */
        public void WritePageToDisk(Page pageToWrite) {

            /**
             * important to note that attribute values for a record must be stored
             * in order they are given, no moving primary key to front, strings to end etc.
             */

            //need to add padding to Char(x)'s to maintain fixed array length for this standard string
        }


        // NEED TO MAKE SURE QUIT IS BEING USED AT ALL TIMES, if we control C
        // we could cause our selves issues with inconsistencies that are
        // hard to deal w

        /**
         * Method iterates through entire Buffer (ArrayList<Page>)
         * checking boolean Page attribute- 'modified' to determine
         * if each successive page needs to be written to hardware,
         * calling WritePageToDisk method on page if so
         *
         * this method is invoked when quit is typed at command line
         * (giant try catch for quit at any time?)
         */
        public void PurgeBuffer() {
            ArrayList<Page> buffer = GetPageBuffer();
            for (Page page : buffer) {
                //when testing can start off by writing all to disk regardless of isModified
                //if (page.getisModified()) {
                    //
                    WritePageToDisk(page);
                    //write each individual char like done in read instead of writeUTF
                //}
            }

        }

    }

}
