package src;/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker
 */

import java.io.*;
import java.util.*;

/**
 * Storage Manager class responsible for all communication and
 * direct access to hardware
 */
public class StorageManager {

    private String rootPath;

    // todo: take in SchemaManager here too
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
    public Table createTable(String name, HashMap<String, String> attributes) {

        // creation of a new table never involves the buffer (there are no pages to start), so the file is created here.
        DataOutputStream output=null;
        try {
            output = new DataOutputStream(new FileOutputStream(rootPath + name));

            // no pages in a new table file, so write 0
            output.writeInt(0);

            // write out the ID of this tale


            output.flush();
            output.close();

            int newId = 0; // todo: ask SchemaManager what ID is available for this new table

            return new Table(newId, name, attributes);


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


    /**
     * The Buffer Manager
     * Has Two Public Methods, GetPage() and PurgeBuffer()
     * The Buffer is in place to ideally reduce read/writes to file system
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
        private ArrayList<Page> GetPageBuffer() { return this.PageBuffer; }


        /**
         * request this page by this table and page number
         * IOException can probably be removed
         *
         * @param tableNumber table num (table is file on disk)
         * @param pageNumber page num
         */
        public void GetPage(int tableNumber, int pageNumber) throws IOException {
            ArrayList<Page> pageBuffer = GetPageBuffer();
            int maxBufferSize = Main.bufferSizeLimit;
            //if block is present in ArrayList already (iterate)
            for (Page page : pageBuffer) {
                if (true) { //todo if page we are looking for
                    page.setLruLongValue(counterForLRU); //update count and increment counterForLRU
                    counterForLRU++;
                    //hand page off, we do not need to read from
                    //disk since already in buffer
                    return;
                }
            }
            //At beginning of program buffer will not be at capacity,
            //so we call read immediately
            if (pageBuffer.size() < maxBufferSize) {
                Page newlyReadPage = ReadPageFromDisk(tableNumber, pageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                pageBuffer.add(newlyReadPage);
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

                //write the LRU page to hardware
                WritePageToDisk(pageBuffer.get(indexOfLRU));

                //read new from hardware into buffer
                Page newlyReadPage = ReadPageFromDisk(tableNumber, pageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                pageBuffer.add(indexOfLRU, newlyReadPage);
            }

        }

        /**
         *
         */
        private Page ReadPageFromDisk(int tableNum, int pageNum) throws IOException {
            Page readPage = new Page();

            /* NEED TO KNOW DATA TYPES FOR parsing
                READING and WRITING data to hardware,
               will grab these data types from catalog
               will individual page know the table scheme for which it belongs too?
            */

            //seek through table file to memory you want and read in page size
            //2D byte array representing records,
            RandomAccessFile file = new RandomAccessFile("filepath TODO", "r");
            byte[][] pageRecords = new byte[0][0];
            //each row in pageRecords represents a record

            file.seek(pageNum * Main.pageSize); //probably will not bring to exact location

            //translate the page into this 2D byte array
            //and then we use that byte array to process each record
            // Loop in the order of data types expected - only will work if i know the order expected
            // of attributes and their types from catalog for a given relation/table/page
                //read int for example
                //read Boolean
                //read char(x)
                    //file.read() method takes can take 3 params and returns next byte of data
                    //file.read(byte[] b, int offset, int length)
                //file also has readInt() readDouble() readBoolean()



            //remove padding for Char(x), stand string type

            return readPage;
        }

        /**
         *
         * @param pageToWrite the page being written to hardware
         */
        private void WritePageToDisk(Page pageToWrite) {



            //need to add padding to Char(x)'s to maintain fixed array length

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
                if (page.getisModified()) {
                    //
                    WritePageToDisk(page);
                }
            }

        }

    }

}
