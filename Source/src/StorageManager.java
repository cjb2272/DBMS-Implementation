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




    /**
     *
     */
    public class BufferManager {


        // The Buffer Itself
        ArrayList<Page> PageBuffer= new ArrayList<Page>();


        /**
         *
         * @param tableNumber table num
         * @param pageNumber page num
         */
        public void GetPage(int tableNumber, int pageNumber) {

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

        /**
         * Method iterates through entire Buffer (ArrayList<Page>)
         * checking boolean Page attribute- 'modified' to determine
         * if each successive page needs to be written to hardware,
         * calling WritePageToDisk method on page if so
         */
        public void PurgeBuffer() {
            
        }

    }

}
