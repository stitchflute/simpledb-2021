package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import java.util.ArrayList;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
	private final TupleDesc td;
	private final int tableid ;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.tableid = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
        // return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableid;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        RandomAccessFile rf = null;
        try{
            rf = new RandomAccessFile(file, "r");
            long offset = BufferPool.getPageSize() * pgNo;
            rf.seek(offset);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            rf.read(bytes, 0, BufferPool.getPageSize());
            HeapPageId pageid = new HeapPageId(tableid, pgNo);
            HeapPage page = new HeapPage(pageid, bytes);
            rf.close();
            return (Page)page;
        } catch (IOException e){
            e.printStackTrace();
        };
        throw new IllegalArgumentException("pid is not valid!");
        // return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId pid = (HeapPageId)page.getId();
        int pgNo = pid.getPageNumber();
        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        long offset = BufferPool.getPageSize() * pgNo;
        rf.seek(offset);
        rf.write(page.getPageData());
        rf.close(); 
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = (int)Math.floor(file.length()*1.0 / BufferPool.getPageSize());
        return num;
        // return 0;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        int num = numPages();
        List<Page> list = new ArrayList<>();
        for(int i = 0; i < num; ++i){
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage)(Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                page.markDirty(true, tid);
                list.add(page);
            }
        }
        if(list.size() == 0){
            HeapPageId npid = new HeapPageId(getId(), num);
            HeapPage blankpage = new HeapPage(npid, HeapPage.createEmptyPageData());
            writePage(blankpage); // write new page to file
            HeapPage npage = (HeapPage)(Database.getBufferPool().getPage(tid, npid, Permissions.READ_WRITE));
            npage.insertTuple(t);
            npage.markDirty(true, tid);
            list.add(npage);
        }
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId)rid.getPageId();
        HeapPage page = (HeapPage)(Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
        page.deleteTuple(t);
        page.markDirty(true, tid);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
        // return null;
    }

    private static class HeapFileIterator implements DbFileIterator{
        private HeapFile file;
        private TransactionId tid;
        private int pageIdx;
        private Iterator<Tuple> currIt;

        public HeapFileIterator(HeapFile f, TransactionId tid){
            this.file = f;
            this.tid = tid;
            pageIdx = -1;
            currIt = null;
        }

        private Iterator<Tuple> getIterator(int pgNo) throws TransactionAbortedException, DbException{
            HeapPageId pid = new HeapPageId(file.getId(), pgNo);
            HeapPage page = (HeapPage)(Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
            return page.iterator();
            // return null;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws TransactionAbortedException, DbException{
            pageIdx = 0;
            currIt = getIterator(pageIdx);
            if (currIt == null) {
                throw new DbException("currIt is null");
            }
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws TransactionAbortedException, DbException{
            // System.out.println("idx: " + pageIdx + file.numPages());
            if(currIt == null){
                return false;
            }
            if(!currIt.hasNext()){
                pageIdx++;
                while(pageIdx < file.numPages()){
                    currIt = getIterator(pageIdx);
                    if(currIt.hasNext())
                        return true;
                    else
                        pageIdx++;
                }
                return false;
            }
            return true;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws TransactionAbortedException, DbException, NoSuchElementException{
            if(hasNext())
                return currIt.next();
            throw new NoSuchElementException("no element");
            // return null;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws TransactionAbortedException, DbException{
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        public void close(){
            currIt = null;
        }
    }

}

