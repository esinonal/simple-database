package simpledb;

import javax.swing.tree.TreeNode;

import simpledb.BufferPool.PageLock;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int pageSize;
    
    // Lock for adding new pages to a HeapFile
    private ReadWriteLock fileLock;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.pageSize = BufferPool.getPageSize();
        this.fileLock = new ReentrantReadWriteLock();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
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
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            int pageNum = pid.getPageNumber();
            raf.seek(pageNum * this.pageSize);
            byte[] b= new byte[this.pageSize];
            raf.read(b);
            return new HeapPage((HeapPageId) pid, b);
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException("FIle was not found");
        } catch (IOException ioe) {
            throw new IllegalArgumentException(("IO exception"));
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPage heapPage = (HeapPage) page;
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
            int pageNum = heapPage.getId().getPageNumber();
            raf.seek(pageNum * this.pageSize);
            byte[] b = page.getPageData();
            assert b.length == BufferPool.getPageSize();
            raf.write(b);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Write failed");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int file_len = (int) this.f.length();
        return file_len / pageSize;
    }

    /* Returns null if no page can be found */
    private HeapPage findFirstEmptyPage(TransactionId tid) throws DbException, TransactionAbortedException {
        int pageCount = 0;
        Set<PageId> pagesChecked;
        while (pageCount < this.numPages()) {
            HeapPageId pageId = new HeapPageId(this.getId(), pageCount);
            try {
            	// Check whether or not the transaction already
            	// has a lock
            	boolean hadLockBefore = false;
            	if (Database.getBufferPool().getTransactionToPage().containsKey(tid)) {
                	hadLockBefore = 
                			Database.getBufferPool().getTransactionToPage().get(tid).contains(pageId);
                		
            	}
            	// Check whether or not the transaction already has
            	// a write lock
            	boolean hadWriteLockBefore = false;
            	if (Database.getBufferPool().getPageToLock().containsKey(pageId)) {
            		hadWriteLockBefore = Database.getBufferPool()
               			.getPageToLock().get(pageId).isWriteLocked() && hadLockBefore;                
            	}
            	// get the page
            	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                // check if we can insert a tuple into this page
            	if (page.getNumEmptySlots() > 0) {
                    return page;
                }
                else {
                	// this is the case specified in the readme, where we 
                	// look without modifying. we can thus unlock straight away
                	if (!hadLockBefore) {
                    	Database.getBufferPool().getTransactionToPage()
        				.get(tid).remove(pageId);
                	}
                	if (!hadWriteLockBefore) {
                    	Database.getBufferPool().getPageToLock()
        				.get(pageId).writeUnlock(tid);
                	}

                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new TransactionAbortedException();
            }
            pageCount++;
        }
        
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
    		throws DbException, IOException, TransactionAbortedException
             {
        HeapPage page = this.findFirstEmptyPage(tid);
        if (page == null) {
//    		// System.out.println("HeapFile insert tuple: inserting tuple to new page");
    		
        	// lock the filelock when adding a new page
        	this.fileLock.writeLock().lock();
            //Num pages is 0 indexed
            HeapPageId pageId = new HeapPageId(this.getId(), this.numPages());
            // add in a lock to the lock manager
            HashMap<PageId, PageLock> pageLockMap = 
            					Database.getBufferPool().getPageToLock(); 
            // add it to the lock hash map
            pageLockMap.put(pageId, Database.getBufferPool().new PageLock(pageId));
            
            // Swap out our fileLock for the page lock now
            // Since we are locked during this whole part,
            // we are still doing 2 phase locking. Think
            // of it like upgrading a read lock to a write lock
            Database.getBufferPool().lockWrapper(Database.getBufferPool().WRITE_LOCK,
            		pageLockMap.get(pageId), tid);
            
            
            // unlock file lock
        	this.fileLock.writeLock().unlock();
          
            // add in a lock to the transaction
            HashMap<TransactionId, Set<PageId>> transactionLockMap = 
            			Database.getBufferPool().getTransactionToPage();
            transactionLockMap.get(tid).add(pageId);     
            HashMap<TransactionId, Set<PageId>> transactionWLockMap = 
        			Database.getBufferPool().getTransactionToPage();
            transactionWLockMap.get(tid).add(pageId);                        
                        
            page = new HeapPage(pageId);
            page.insertTuple(t);
            this.writePage(page);
            
            // adding into our page hash
            int hashCode = pageId.hashCode();
            Database.getBufferPool().pageHash.put(hashCode, page);
            Database.getBufferPool().putInQueue(hashCode);

        } else {
            page.insertTuple(t); // changes record id
        }
        page.markDirty(true, tid);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) 
    	throws DbException, TransactionAbortedException {
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = null;
		try {
			page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        try {
			page.deleteTuple(t);
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        page.markDirty(true, tid);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        ArrayList<HeapPageId> pageIdPointers = new ArrayList<>();
        int tableId = this.getId();
        int numPages = this.numPages();
        int i = 0;
        HeapPageId pageIdKey;
        while (i < numPages) {
            pageIdKey = new HeapPageId(tableId, i);
            pageIdPointers.add(pageIdKey);
            i++;
        }

        return new DbFileIterator() {
            private Iterator<Tuple> iter = null;
            private ArrayList<HeapPageId> idPointers = pageIdPointers;
            private int numIterators = idPointers.size();
            private int onIterator = 0;
            private TransactionId transacId = tid;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (this.idPointers == null) {
                    throw new TransactionAbortedException();
                }
                this.onIterator = 0;

                try {
                    try {
						this.iter = this.getNextPageIterator();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						// System.out.println("Before the output");
						e.printStackTrace();
						// System.out.println("After the output");
					}
                } catch (NullPointerException e) {
                    throw new DbException("Null reference");
                }
            }

            @Override
            public boolean hasNext() throws DbException,
            	TransactionAbortedException {
                if (this.iter == null) {
                    return false;
                }
                if (!this.iter.hasNext()) {
                    // check if there's another iterator to move on to
                    while (this.onIterator < this.numIterators - 1) {
                        this.onIterator++;
                        try {
                        	this.iter = this.getNextPageIterator();
                        }
                        catch (IOException e) {
    						// TODO Auto-generated catch block
    						// System.out.println("Before the output");
    						e.printStackTrace();
    						// System.out.println("After the output");
    					}
                        boolean ret = this.iter.hasNext();
                        if (ret) {
                            return true;
                        }
                    }
                    return false;
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.iter == null) {
                    throw new NoSuchElementException("iterator is null");
                }
                try {
                    //hasNext sets us to the correct next iterator
                    // if it returns false, means there is no next tuple
                    if (!this.hasNext()) {
                        throw new NoSuchElementException("No next element");
                    }
                    return this.iter.next();
                } catch (NullPointerException e) {
                    throw new DbException("null pointer exception");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public void close() {
                this.iter = null;
            }

            private Iterator<Tuple> getNextPageIterator() throws DbException, TransactionAbortedException, IOException {
                Iterator<Tuple> pageIterator = null;
                
                    HeapPageId pid = idPointers.get(onIterator);
                    HeapPage page =
                            (HeapPage) Database.getBufferPool().getPage(this.transacId, pid, Permissions.READ_WRITE);
                    pageIterator = page.iterator();
  
                return pageIterator;
            }
        };
    }

}

