package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;


class LockManager{

    public enum LockType{
        IX, IS
    }

    public class Lock{
        LockType type;
        TransactionId tid;
        public Lock(TransactionId tid, LockType t){
            this.type = t;
            this.tid = tid;
        }
    }

    private ConcurrentHashMap<PageId, Set<Lock>> holder;
    private final static long TIMEOUT = 100;

    public LockManager(){
        this.holder = new ConcurrentHashMap<>();
    }

    public synchronized void printLock(TransactionId tid, PageId pid, LockType type){
        if(holder.containsKey(pid)){
            Set<Lock> locks = holder.get(pid);
            System.out.println(locks.size());
            for (Lock lock : locks) {
                System.out.printf("pid: %s tid: %s type: %s\n", pid.toString(), lock.tid.toString(), lock.type);
            }
        }   
    }

    /**********************************
    1. pid没有锁，直接加锁
    2. pid上有锁：
        a. tid上有锁
            请求读锁，直接返回
            请求写锁
                已有为写锁直接返回，
                已有为读锁
                    若有其他读锁，阻塞，
                    无其他读锁，锁升级
        b. tid上无锁
            请求读锁
                其他tid上有写锁，阻塞
                其他tid上无写锁，加读锁
            请求写锁
                阻塞
    *********************************/


    public synchronized boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) throws InterruptedException, DbException{
        LockType type = (perm == Permissions.READ_ONLY) ? LockType.IS : LockType.IX;
        final String thread = Thread.currentThread().getName();
        // printLock(tid, pid, type);
        Set<Lock> locks;
        if(!holder.containsKey(pid)){ // no lock in pid
            locks = new HashSet<>();
            Lock newLock = new Lock(tid, type);;
            locks.add(newLock);
            holder.put(pid, locks);
            // System.out.println(thread + ": the " + pid + " have no lock, transaction" + tid + " require " + type + ", accept");
            return true;
        }
        else{ // has lock in pid
            // System.out.println("111");
            locks = holder.get(pid);
            Lock sameLock = null;
            for (Lock lock : locks) {
                if(lock.tid.equals(tid)){
                    sameLock = lock;
                    break;
                }
            }
            if(sameLock != null){ // has lock in tid
                if(type == LockType.IS){ // require read lock
                    // System.out.println(thread + ": the " + pid + " have one lock with same txid, transaction" + tid + " require read lock accept");
                    return true;
                }
                else{ // require write lock
                    if(sameLock.type == LockType.IX){
                        // System.out.println(thread + ": the " + pid + " have write lock with same txid, transaction" + tid + " require write lock accept");
                        return true;
                    }
                    else if(locks.size() > 1){
                        // System.out.println(thread + ": the " + pid + " have many read locks, transaction" + tid + " require write lock, abort!!!");
                        // wait(TIMEOUT);
                        return false;
                    }
                    else{
                        locks.remove(sameLock);
                        sameLock.type = LockType.IX;
                        locks.add(sameLock);
                        holder.put(pid, locks);
                        // System.out.println(thread + ": the " + pid + " have read lock with same txid, transaction" + tid + " require write lock, accept and upgrade!!!");
                        return true;
                    }
                }
            }
            else{ // no lock in tid
                if(type == LockType.IS){
                    Lock hasWrite = (Lock)locks.toArray()[0];
                    if(hasWrite.type == LockType.IX){
                        // System.out.println(thread + ": the " + pid + " have write lock with diff txid, transaction" + tid + " require read lock, wait!!!");
                        // wait(TIMEOUT);
                        return false;
                    }
                    else{
                        Lock newLock = new Lock(tid, type);
                        locks.add(newLock);
                        holder.put(pid, locks);
                        // System.out.println(thread + ": the " + pid + " have read lock with diff txid, transaction" + tid + " require read lock, accept");
                        return true;
                    }
                }
                else{
                    // System.out.println(thread + ": the " + pid + " have lock with diff txid, transaction" + tid + " require write lock, wait!!!");
                    // wait(TIMEOUT);
                    return false;
                }
            }
        }
    }

    public synchronized boolean releaseLock(PageId pid, TransactionId tid){
        if(holder.containsKey(pid)){
            Set<Lock> locks = holder.get(pid);
            for (Lock lock : locks) {
                if(lock.tid.equals(tid)){
                    locks.remove(lock);
                    if(locks.size() == 0)  
                        holder.remove(pid);
                    else
                        holder.put(pid, locks);
                    // this.notifyAll();
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized boolean holdsLock(PageId pid, TransactionId tid){
        if(holder.containsKey(pid)){
            Set<Lock> locks = holder.get(pid);
            for (Lock lock : locks) {
                if(lock.tid.equals(tid))
                    return true;
            }
        }
        return false;
    }

}

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private ConcurrentHashMap<PageId, Page> pages;
    private int numPages;
    private LockManager lock;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.lock = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        long bgn = System.currentTimeMillis();
        while(true){
            try{
                if(lock.acquireLock(pid, tid, perm))
                    break;
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }
            if (System.currentTimeMillis() - bgn > 100) 
                throw new TransactionAbortedException();
        }
        // System.out.println(acquired);
        // if(!acquired)
            // throw new DbException("acquired lock failed");
        Page page = null;
        if(pages.containsKey(pid))
            page = pages.get(pid);
        else{
            if(pages.size() >= numPages)
                evictPage();
            page =  Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pages.put(pid, page);
        }
        // lock.releaseLock(pid, tid);
        return page;
        // return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(p, tid);
        // return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit){
        // some code goes here
        // not necessary for lab1|lab2
        try{
            if(commit){
                flushPages(tid);
            }
            else{
                rollPages(tid);
            }
            for (PageId p : pages.keySet()) {
                if(holdsLock(tid, p))
                    unsafeReleasePage(tid, p);
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        List<Page> list = file.insertTuple(tid, t);
        for(Page p: list){
            p.markDirty(true, tid);
            if(pages.size() > numPages)
                evictPage();
            pages.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> list = file.deleteTuple(tid, t);
        for(Page p: list){
            p.markDirty(true, tid);
            if(pages.size() > numPages)
                evictPage();
            pages.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (ConcurrentHashMap.Entry entry : pages.entrySet()) {
            flushPage((HeapPageId)(entry.getKey()));
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPage p = (HeapPage)pages.get(pid);
        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(p);
        // p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pages.keySet()) {
            Page p = pages.get(pid);
            if(p.isDirty() == tid)
                flushPage(pid);
        }
    }

    public synchronized  void rollPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pages.keySet()) {
            Page p = pages.get(pid);
            if(p.isDirty() == tid){
                DbFile file =  Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page pageFromDisk = file.readPage(pid);
                pages.put(pid, pageFromDisk);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<ConcurrentHashMap.Entry<PageId, Page>> entries = pages.entrySet().iterator();
        while(entries.hasNext()){
            ConcurrentHashMap.Entry<PageId, Page> entry = entries.next();
            if(entry.getValue().isDirty() != null)
                continue;
            HeapPageId pid = (HeapPageId)entry.getKey();
            try{
                flushPage(pid);
                discardPage(pid);
                return;
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
        throw new DbException("all page is dirty, can't evict");
    }

}
