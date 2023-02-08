/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker
 */

import java.util.*;

/**
 * Main Phase 1 Class, Storage Manager
 */
public class StorageManager {


    // Decide upon max buffer size and set here? todo



    /**
     * The Buffer Manager
     * Has Two Public Methods, GetPage() and PurgeBuffer()
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
         * @param tableNumber table num
         * @param pageNumber page num
         */
        public void GetPage(int tableNumber, int pageNumber) {
            ArrayList<Page> pageBuffer = GetPageBuffer();
            int bufferSize = pageBuffer.size();
            //if block is present in ArrayList already (iterate)
                //update count and increment counterForLRU
                //hand page off, we do not need to read from
                //disk since already in buffer
            //else
                //At beginning of program buffer will not be at capacity,
                //so we call read immediately
                //if bufferSize < MAXSIZE
                    // newlyReadPage = ReadPageFromDisk()
                    // newlyReadPage.lruLongValue = counterForLRU;
                    // counterForLRU++;
                    // pageBuffer.add(newlyReadPage);
                //else
                    // int indexOfLRU;
                    //int tempLRUCountVal = POSITIVE INFINITY
                    //for (int curPageIndex = 0; pageIndex < size; pageIndex++) {
                        //int lruCountVal = pageBuffer.get(curPageIndex).lruLongValue;
                        //if (lruCountVal < tempLRUCountVal) {
                            //tempLRUCountVal = lruCountVal;
                            //indexOfLRU = curPageIndex;
                        //}
                    //}

                    // call WritePageToDisk(pageBuffer.get(indexOfLRU))

                    // newlyReadPage = ReadPageFromDisk()
                    // newlyReadPage.lruLongValue = counterForLRU;
                    // counterForLRU++;
                    // pageBuffer.add(indexOfLRU, newlyReadPage);


        }

        /**
         *
         */
        private Page ReadPageFromDisk() {
            Page readPage = new Page();

            return readPage;
        }

        /**
         *
         * @param pageToWrite the page being written to hardware
         */
        private void WritePageToDisk(Page pageToWrite) {

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
            //Loop through buffer
                //grab currentPage
                //if currentPage.isModified == true
                    //call WritePageToDisk()
        }

    }

}
