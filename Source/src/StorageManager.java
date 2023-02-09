/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker
 */

import java.util.*;

/**
 * Storage Manager class responsible for all communication and
 * direct access to hardware
 */
public class StorageManager {



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
         * @param tableNumber table num (table is file on disk)
         * @param pageNumber page num
         */
        public void GetPage(int tableNumber, int pageNumber) {
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
                Page newlyReadPage = ReadPageFromDisk();
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
                Page newlyReadPage = ReadPageFromDisk();
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                pageBuffer.add(indexOfLRU, newlyReadPage);
            }

        }

        /* NEED TO KNOW DATA TYPES FOR parsing
           READING and WRITING data to hardware,
           will grab these data types from catalog


         */

        /**
         *
         */
        private Page ReadPageFromDisk() {
            Page readPage = new Page();

            //seek through table file to memory you want and read in page size
            //byte array byte array byte arrays


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
