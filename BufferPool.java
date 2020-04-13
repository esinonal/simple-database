package simpledb;

import java.io.*;
 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.*;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;



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
    public static final int READ_LOCK = 1;
    public static final int WRITE_LOCK = 2;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    ConcurrentHashMap<Integer, Page> pageHash;
    public PriorityQueue<Integer> lruQueue;
    
    //Holds Page to its Lock. May or may not be locked.
    private HashMap<PageId, PageLock> pageToLock; 
    //The transID shows the pages that are locked, from which we can get the Locks.
    private HashMap<TransactionId, Set<PageId>> transactionToPage; 
    
    private HashMap<TransactionId, Set<PageId>> transactionToWLocks; 
    private HashMap<TransactionId, Set<PageId>> transactionToRLocks;
    
    
    // a lock attained before grabbing any lock
    public Lock transitionLock;
    
    /**
     * Helper class for lock manager
     * */
    public class PageLock {

    	// the lock itself
        public ReadWriteLock lock;
        
        // boolean to see if lock is read locked
        public Boolean readLocked;
        // boolean to see if lock is write locked
        public Boolean writeLocked;
        
        // the page
        private PageId pid;
        
        // flag to see if write lock is attempting to grab
        // a read lock, so that read locks don't hog
        private boolean acquiringWriteLock;
        
        // Keep track of all transactions that have a read lock
        public HashSet<TransactionId> readLockTransactions;
        
        public void acquireLock(int lockType, TransactionId tid) throws IOException 
       {
        	// deadlock resolution method
        	long startTime = System.currentTimeMillis();
        	
        	boolean locked = true;
        	
        	while (locked == true) {
        		long currentTime = System.currentTimeMillis();
        		
        		// if 150 milliseconds have passed since we 
        		// started trying to get the lock, we call it a deadlock
        		if (currentTime - startTime >= 150) {
        			
        			// handle deadlock
        			try {
						throw new TransactionAbortedException();
					} catch (TransactionAbortedException e) {
						// System.out.println("--------------------\n");
						// System.out.println("Got new deadlock!\n");
						// System.out.println("--------------------\n");
						// e.printStackTrace();
						// homicidal method: abort all other transactions
						for (TransactionId someTid : transactionToPage.keySet()) {
	        				if (someTid.myid != tid.myid) {
	                			transactionComplete(someTid, false);	                			
	        				}
	        			}
						return;
					}        			
        		}
            	if (lockType == READ_LOCK) {
            		locked = this.writeLocked;
            	}
            	else if (lockType == WRITE_LOCK) {
            		locked = this.writeLocked || this.readLocked;
            	}
            	// pause after each check
            	try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        }
        
        
        public PageLock(PageId pid) {
            this.lock = new ReentrantReadWriteLock();
            this.readLocked = false;
            this.writeLocked = false;
            this.pid = pid;
            this.acquiringWriteLock = false;
            this.readLockTransactions = new HashSet<TransactionId>();
            
        }

        public boolean isReadLocked() {
            return this.readLocked;
        }
        
        public boolean isWriteLocked() {
            return this.writeLocked;
        }
        
        public void writeLock(TransactionId tid) throws IOException {
    		// System.out.println("writeLock: tryna get a lock. TID: " + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");

        	if (this.readLocked) {
        		if (transactionToRLocks.containsKey(tid)) {
            		if (transactionToRLocks.get(tid).contains(pid)) {
            			// simply upgrade
            			readUnlock(tid);
            			acquireLock(WRITE_LOCK, tid);
            		}
            		else {
            			acquireLock(WRITE_LOCK, tid);
            		}
        		}

        		else {
//        			// System.out.println("writeLock: already read locked");
            		acquiringWriteLock = true;
            		acquireLock(WRITE_LOCK, tid);
            		acquiringWriteLock = false;
            		
        		}
        	}
        	// if we have already write locked this tid
        	else if (this.writeLocked) {
        		if (transactionToWLocks.containsKey(tid)) {
            		if (transactionToWLocks.get(tid).contains(pid)) {
            			// pass, we are already locked!
            		}
            		else {
            			acquireLock(WRITE_LOCK, tid);
            		}
        		}

        		// now we wait:
        		else {
            		acquireLock(WRITE_LOCK, tid);            		
        		}
        	}
        	
    		// System.out.println("writeLock: got a lock. TID: " + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");

        	this.writeLocked = true;
//        	// wrapper after doing any locks
//    		BufferPool.this.transitionLock.unlock();
        }
        
        public void readLock(TransactionId tid) throws IOException {
    		// System.out.println("readLock: attempting to get lock for TID: "
    		//				   + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");

    		if (this.writeLocked) {
        		// if we are the one with a write lock,
        		// no need to do anything
        		if (transactionToWLocks.containsKey(tid)) {
            		if (transactionToWLocks.get(tid).contains(pid)) {
            			// pass            		
            		}
            		else {
                    	acquireLock(READ_LOCK, tid);
            		}
            	}
        		else {
                	acquireLock(READ_LOCK, tid);
        		}
        	}
        	else if (this.readLocked) {
        		// if we already have a read lock, we are fine
        		if (transactionToRLocks.containsKey(tid)) {
        			if (transactionToRLocks.get(tid).contains(pid)) {
        				// pass, we are already locked
        			}
        			else {
                    	acquireLock(READ_LOCK, tid);
        			}
        		}
        		// if *a* transaction is currently trying to get
        		// a read lock first, wait for it to finish
        		// by trying to grab a write lock first instead
        		else if (acquiringWriteLock = true) {
//            		// System.out.println("readLock: stuck here");
//
//        			acquireLock(WRITE_LOCK, tid);  
//            		this.readLockTransactions.add(tid);
//        			this.readLocked = true;
//        			writeUnlock(tid);
        		}
        		// else we can just join the masses of
        		// read locked transactions
        		else {
        			// pass
        		}
        	}
        	// neither read or write locked
        	else {
            	acquireLock(READ_LOCK, tid);
        	}
        	
        	// got the lock
    		// System.out.println("\nreadLock: got the lock for TID: " + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");
        	this.readLockTransactions.add(tid);
        	this.readLocked = true;
        	
//        	// wrapper after doing any locks
//    		BufferPool.this.transitionLock.unlock();
        }
        
        public void writeUnlock(TransactionId tid) {
            // System.out.println("writeLock: unlocked by TID: " + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");

        	this.writeLocked = false;
        	
        }
        
        public void readUnlock(TransactionId tid) {
            // System.out.println("readLock: unlocked by TID: " + tid.myid);
    		// System.out.println("PID: " + pid.getPageNumber());
    		// System.out.println("");
        	this.readLockTransactions.remove(tid);
        	if (this.readLockTransactions.isEmpty()) {
        		this.readLocked = false;
        	}
        }
    }
    
    
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageHash = new ConcurrentHashMap<>();
        this.lruQueue = new PriorityQueue<>(numPages);
        this.pageToLock = new HashMap<>();
        this.transactionToPage = new HashMap<>();
        this.transitionLock = new ReentrantLock();
        this.transactionToWLocks = new HashMap<>(); 
        this.transactionToRLocks = new HashMap<>();
    }
    
    // so that only one thread can ever be trying to lock at one
    // time.
    public void lockWrapper(int lockType, PageLock lock, TransactionId tid) 
    		throws IOException 
    		 {
    	synchronized(this) {

    		if (lockType == READ_LOCK) {
    			lock.readLock(tid);
    		}
    		else if (lockType == WRITE_LOCK) {
//    			// System.out.println("Here is the tid: " + tid.myid);
    			lock.writeLock(tid);
    		}
    		return;
    	}
    }
    
    public HashMap<PageId, PageLock> getPageToLock() {
    	return pageToLock;
    }
    
    public HashMap<TransactionId, Set<PageId>> getTransactionToPage() {
    	return transactionToPage;
    }

    private boolean replaceExistingPage() {
        return (this.pageHash.size() == this.numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() { BufferPool.pageSize = DEFAULT_PAGE_SIZE; }

    public static Page pidToPage (PageId pid) {
    	int tableID = pid.getTableId();
    	DbFile file = Database.getCatalog().getDatabaseFile(tableID);
    	Page thePage = file.readPage(pid);
    	return thePage;
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
     * @throws IOException 
     * @throws Exception 
     */
    // NOTE! Might need to add in some logic to prevent threads from getting 
    // the same lock twice? Dunno how it will handle this
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException, IOException {
        if (perm != Permissions.READ_ONLY && 
            perm != Permissions.READ_WRITE) {
            throw new TransactionAbortedException();
        }
        // wrapper for locking, to prevent lock
        // swooping
    	this.transitionLock.lock();

        
        
        // 1: Get the lock:
        PageLock pageLock;     
    	// if we have already locked this page at least once
    	if (pageToLock.containsKey(pid)) {
    		pageLock = pageToLock.get(pid);
    	}
    	else {
    		pageLock = new PageLock(pid);
    		pageToLock.put(pid, pageLock);
    	}
        
    	
    	// 2: Lock the lock: 
    	// if we only need read permission, get read lock
    	// else, get write lock
        if (perm == Permissions.READ_ONLY) {

    		lockWrapper(READ_LOCK, pageLock, tid);        	
        }
        // Write Lock needed
        else {
        	// checks if page is already write locked
        	// for this transaction
        	if (transactionToPage.containsKey(tid)) {
        		if (transactionToPage.get(tid).contains(pid)) {
        			if (pageToLock.get(pid).isWriteLocked()) {
        				// pass
        			}
        			else if (pageToLock.get(pid).isReadLocked()) {
        				// upgrade
                    	pageLock.readUnlock(tid);
                    	// System.out.println("Grabbing write lock");
                    	// pageLock.writeLock();
        				lockWrapper(WRITE_LOCK, pageLock, tid);
                    	// System.out.println("Got write lock");
                    //	pageLock.readLock();
        			}
        		}
        		else {
        			// // System.out.println("transaction in hash map,
        			// but never locked this page");
        			lockWrapper(WRITE_LOCK, pageLock, tid);
        		}
        	}
        	else {
        		lockWrapper(WRITE_LOCK, pageLock, tid);
        	}
        }

        
        
        
        // 3: Add page to list of pages that are "locked"
        
        // If we have locked on this transaction before,
        // we already have a hash map
        if (transactionToPage.containsKey(tid)) {
        	Set<PageId> currentPagesList = transactionToPage.get(tid); 
        	currentPagesList.add(pid); //Add the new page, if it can be added.
        	transactionToPage.put(tid, currentPagesList);
        	
        	// add to Read or Write transaction - lock hash map
        	if (perm == Permissions.READ_ONLY) {
        		if (transactionToRLocks.containsKey(tid)) {
            		transactionToRLocks.get(tid).add(pid);    
            		transactionToPage.get(tid).add(pid);
        		}
        		else {
                	Set<PageId> currentRLocks = new HashSet<PageId>(); 
                	currentRLocks.add(pid);
        			transactionToRLocks.put(tid, currentRLocks);
        		}
        	}
        	else if (perm == Permissions.READ_WRITE) {
        		if (transactionToWLocks.containsKey(tid)) {
            		transactionToWLocks.get(tid).add(pid); 
            		transactionToPage.get(tid).add(pid);
        		}
        		else {
                	Set<PageId> currentWLocks = new HashSet<PageId>(); 
                	currentWLocks.add(pid);
        			transactionToRLocks.put(tid, currentWLocks);
        		}
        	}
        }
        // else, we need to put in a new hash map for pages
        else {
        	Set<PageId> currentPagesList = new HashSet<PageId>();
        	currentPagesList.add(pid); //Add the new page, if it can be added.
        	transactionToPage.put(tid, currentPagesList);
        	if (perm == Permissions.READ_ONLY) {
        		transactionToRLocks.put(tid, currentPagesList);
        	}
        	else if (perm == Permissions.READ_WRITE) {
        		transactionToWLocks.put(tid, currentPagesList);
        	}
        }
        
        // grab page from page hash
        Page page = this.pageHash.get(pid.hashCode()); //their code

        
        int hashCode = pid.hashCode();

        if (page == null) { // onwards is their code.
//        	// System.out.println("page not in pool");
        	
            if (this.replaceExistingPage()) {
                this.evictLRUPage();
            }
            // Page not in pool: Grab it from database.
            page = Database.getCatalog()
                    .getDatabaseFile(pid.getTableId()).readPage(pid);
            this.pageHash.put(hashCode, page);
        }
        this.putInQueue(hashCode);
        // this.pageHash.put(hashCode, page);
        // wrapper for locking, to prevent lock
        // swooping
    	this.transitionLock.unlock();
        return page;

    }
   

    void putInQueue(int hashCode) {
        // puts element at end if accessed again
        this.lruQueue.remove(hashCode);
        this.lruQueue.add(hashCode);
    }

    //// so, cant use this one to take top to see if dirty or not. 
    ////This function will actually flush the page. if you use it to access the top 
    /*
    private void evictLRUPage() { //This is function THEY gave us; only evicts LRU.
        try {                       
            int head = this.lruQueue.remove();
            Page page = this.pageHash.get(head);
            this.flushPage(page.getId());
            this.pageHash.remove(head);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
    
  //Our old evict LRU page is not good enough;
	// we need to evict LRU page that is ALSO not dirty. 
    private void evictLRUPage() throws DbException { 
   // 	try {
    		Page page = null;
        	boolean found = false;
        	int targetPageHash = 0;
        	
        	for (Integer item : lruQueue){   //Gets ths least recently used first. 
        		if (!pageHash.containsKey(item)) {
        			// System.out.println("evictLRUPage: pageHash doesn't contain int");
        		}
        		page = pageHash.get(item);
        		TransactionId isDirty = page.isDirty();
        		if (isDirty == null) { //is not dirty; done.
        			targetPageHash = item;
        			found = true;
        			break;
        		}
        		//if not, keep looping.
        	}
        	// This is the page that is least recently used that is NOT dirty.
        	// If not dirty, we dont need to flush.
    		// Evict it, this means remove from BP, which means 
        	// remove from LRUQueue and pageHash.
        	if (found) { 
//        		// System.out.println("evictLRUPage: found a non dirty page");
        		//Remove from LRUQueue
        		//Remove from pageHAsh
        	//	discardPage(page.getId());
        		//try {
					this.discardPage(page.getId());
				//} catch (IOException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				//}
        		this.pageHash.remove(targetPageHash);        	
        		this.lruQueue.remove(targetPageHash);
        	}
        	else {
        		//None found.
        		throw new DbException("Cannot evict a page!");
        		//return;
        	}
    	}
//    	catch (Exception e){
//    		e.printStackTrace();
//    	}
    

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        
    	// 1: Unlock the lock itself. 
    	if (pageToLock.containsKey(pid)) {
    		PageLock pageLock = pageToLock.get(pid);
        	
    		// might be a problem with unlocking both of these?
        	// depending on if we can unlock if it's already unlocked...
    		// pageLock.readUnlock();
    		

    		// see if any other transaction has a read lock on the page
    		boolean canUnlock = true;
    		for (TransactionId someTid : transactionToRLocks.keySet()) {
    			if (someTid.myid != tid.myid && transactionToRLocks.get(someTid).contains(pid))
    			{
    				canUnlock = false;
    			}
    		}
    		if (canUnlock) {
        		pageLock.readUnlock(tid);	
        		pageLock.writeUnlock(tid);	
    		}
        	// 2: Then remove page as page no longer locked.
    		if (transactionToRLocks.containsKey(tid)) {
    			// remove from r lock tracker
            	transactionToRLocks.get(tid).remove(pid);
            	// check to see if can remove from total tracker
        		if (transactionToWLocks.containsKey(tid)) {
        			if (!transactionToWLocks.get(tid).contains(pid)) {
                    	transactionToPage.get(tid).remove(pid);
        			}
        		}
            	
    		}
    		if (transactionToWLocks.containsKey(tid)) {
            	transactionToWLocks.get(tid).remove(pid);
            	// check to see if can remove from total tracker
        		if (transactionToRLocks.containsKey(tid)) {
        			if (!transactionToRLocks.get(tid).contains(pid)) {
                    	transactionToPage.get(tid).remove(pid);
        			}
        		}
    		}

    	}
    	else {
    		throw new IllegalArgumentException("releasePage: page not" +
    										   " in lock manager" +
    										   " (never locked even once?)");
    	}	
    }


    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return (transactionToRLocks.get(tid).contains(p) || 
        		transactionToWLocks.get(tid).contains(p));
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @throws IOException 
     */
    public void transactionComplete(TransactionId tid) throws IOException  {
    	// System.out.println("\n\n000000000000000000000000000000000000000\n");
    	// System.out.println("transactionComplete is called - commit\n");
    	
//    	When you commit, you should flush dirty pages
//  	  associated to the transaction to disk. When you abort, you should revert
//  	  any changes made by the transaction by restoring the page to its on-disk
//  	  state.
//
//  	Whether the transaction commits or aborts, you should also release any state the
//  	  BufferPool keeps regarding
//  	  the transaction, including releasing any locks that the transaction held.
    	
    	
        //This function always commits. 
    	
    	//Need to flush the dirty pages on BP to disk.
    	flushPages(tid); //Each page gets flushed separately.
    	
    	// Release locks:
    	HashSet<PageId> listOfPages = new HashSet<PageId>(transactionToPage.get(tid));
    	

    	for (PageId pageId : listOfPages) {
    		releasePage(tid, pageId);
    		// discardPage(pageId);
    	}
    	// System.out.println("000000000000000000000000000000000000000\n\n\n");

    }


    
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     * @throws IOException 
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException
         {

    //	this.transitionLock.lock();
//    	When you commit, you should flush dirty pages
//    	  associated to the transaction to disk. 
    	
    	// When you abort, you should revert
//    	  any changes made by the transaction by restoring the page to its on-disk
//    	  state.
//
//    	Whether the transaction commits or aborts, you should also release any state the
//    	  BufferPool keeps regarding
//    	  the transaction, including releasing any locks that the transaction held.
    	
    	
    	if (commit) {
    		transactionComplete(tid); //Pass to the commit function
    		return;
    	}
    	else {
        	// System.out.println("1111111111111111111111111111111111111111111\n");
        	// System.out.println("transactionComplete is called - abort\n");
    		//Implement the do not commit part:
    		
    		// Restore page to on-disk state. 
    		// (this is a discard, EITHER, you do this discard, OR the flush.)
    		
    		if (transactionToPage.containsKey(tid)) {
        		HashSet<PageId> setOfPages = new HashSet<PageId>(transactionToPage.get(tid));
            	for (PageId pageId : setOfPages) {
            		discardPage(pageId); //Removes from: pageHash, lruQueue
            		releasePage(tid, pageId); // release locks
            	}
    		}
        	// System.out.println("1111111111111111111111111111111111111111111\n");

    	}

    //	this.transitionLock.unlock();

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
        throws DbException, IOException, TransactionAbortedException
        {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> aLPage = file.insertTuple(tid, t);
        this.putPagesInCache(aLPage);
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
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> aLPage = file.deleteTuple(tid, t);
        this.putPagesInCache(aLPage);
    }

    private void putPagesInCache(ArrayList<Page> alPage) {
        Iterator<Page> alPageIterator = alPage.iterator();
        while (alPageIterator.hasNext()) {
            Page page = alPageIterator.next();
            this.pageHash.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Map.Entry<Integer, Page> entry : this.pageHash.entrySet()) {
            PageId pid = entry.getValue().getId();
            this.flushPage(pid);
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
    	
        this.pageHash.remove(pid.hashCode());
        this.lruQueue.remove(pid.hashCode()); 
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     * @throws IOException 
     * @throws DbException 
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = this.pageHash.get(pid.hashCode());
        if (page == null) {
            throw new IOException("Can't evict a page!");
        }
        if (page.isDirty() != null) {
//        	// System.out.println("Another page being flushed: PID: " + pid.getPageNumber());
    		// System.out.println("The following page has been flushed: " + pid.getPageNumber());

            page.markDirty(false, null);
            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            try {
				file.writePage(page);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }

    /** Write all pages of the specified transaction to disk.
     * @throws IOException 
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {

        // some code goes here
        // not necessary for lab1|lab2
    	HashSet<PageId> listOfPages = new HashSet<PageId>(transactionToPage.get(tid));
//    	if (transactionToRLocks.containsKey(tid)) {
//        	HashSet<PageId> listOfPages1 = new HashSet<PageId>(transactionToRLocks.get(tid));
//        	listOfPages.addAll(listOfPages1);
//    	}
//    	if (transactionToWLocks.containsKey(tid)) {
//        	HashSet<PageId> listOfPages2 = new HashSet<PageId>(transactionToWLocks.get(tid));
//        	listOfPages.addAll(listOfPages2);
//    	}

    	
    	for (PageId pageId : listOfPages) {
    		flushPage(pageId);
    	}

    }

    
    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage(PageId pageId) throws IOException {
        try {
            this.flushPage(pageId);
            this.discardPage(pageId);
        } catch (IOException ioe) {
            throw new IOException("could not flush page");
        }
    }
}




