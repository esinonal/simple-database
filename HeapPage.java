package simpledb;

import java.util.*;
import java.io.*; 

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    private final HeapPageId pid;
    private final TupleDesc td;
    private final byte header[];
    public final Tuple tuples[];
    public final int numSlots;
    private TransactionId dirtyTid;

    private byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */

    /**
     * Creates new empty heap page that does not read in any data
     * @param id
     * @throws IOException
     */
    public HeapPage(HeapPageId id) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = this.getNumTuples();
        this.header = new byte[getHeaderSize()];
        this.tuples = new Tuple[this.numSlots];
        this.dirtyTid = null;
        setBeforeImage();
    }

    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.dirtyTid = null;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++) {
            header[i] = dis.readByte();
        }
	    tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        return (int) Math.floor((float) (BufferPool.getPageSize() * 8)/(this.td.getSize() * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.ceil((double)this.numSlots / 8);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
//    	// System.out.println("\n Entering HeapPage deleteTuple \n");
//    	// System.out.println("\n tuple to delete: " + ((IntField) t.getField(0)).getValue());
    	// System.out.println("\n\nKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK \n");
    	// System.out.println("Entering HeapPage deleteTuple \n");
    	// System.out.println("\nTuple-to-delete value: " +  ((IntField) t.getField(0)).getValue());
    	// System.out.println("");

        RecordId recId = t.getRecordId();
        int tupleNumber = recId.getTupleNumber();
        if (recId.getPageId() != this.pid) {
            throw new DbException("Tuple not on this page");
        }
        if (!this.isSlotUsed(tupleNumber)) {
            throw new DbException("Tuple slot already empty");
        }
        this.markSlotUsed(tupleNumber, false);
        this.tuples[tupleNumber] = null;
        t.setRecordId(new RecordId(t.getRecordId().getPageId(), -1));
//    	// System.out.println("\n Iterating through tuple list now for pid: " + this.pid.getPageNumber());
//    	// System.out.println("\n");
//
//        for (Tuple tup : tuples) {
//        	if (tup != null)
//        		// System.out.println("         Tuple value: " +  ((IntField) tup.getField(0)).getValue());
//        }
//    	// System.out.println("\n Done Iterating through tuple list \n");
//    	// System.out.println("\n Exiting HeapPage deleteTuple \n");
    	// System.out.println("Iterating through tuple list now for pid: " + this.pid.getPageNumber());
    	// System.out.println("");

//        for (Tuple tup : tuples) {
//        	//if (tup != null)
//        		// System.out.println("         Tuple value: " +  ((IntField) tup.getField(0)).getValue());
//        }
    	// System.out.println("\n Done Iterating through tuple list \n");
    	// System.out.println("Exiting HeapPage deleteTuple \n");
    	// System.out.println("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK \n\n\n");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
    	// System.out.println("\n\nVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV \n");
    	// System.out.println("Entering HeapPage insertTuple \n");
    	// System.out.println("\nTuple-to-insert value: " +  ((IntField) t.getField(0)).getValue());
    	// System.out.println("");

        if (!t.getTupleDesc().equals(this.td)) {
            throw new DbException("Tuple description is a mismatch");
        }
        if (this.getNumEmptySlots() == 0) {
            throw new DbException("Page is full, cannot add tuple");
        }
        int slot = 0;
        while (isSlotUsed(slot)) { slot++; }
        this.markSlotUsed(slot, true);
        t.setRecordId(new RecordId(this.pid, slot));
        this.tuples[slot] = t;
    	// System.out.println("Iterating through tuple list now for pid: " + this.pid.getPageNumber());
    	// System.out.println("");

//        for (Tuple tup : tuples) {
//        	if (tup != null)
//        		// System.out.println("         Tuple value: " +  ((IntField) tup.getField(0)).getValue());
//        }
    	// System.out.println("\n Done Iterating through tuple list \n");
    	// System.out.println("Exiting HeapPage insertTuple \n");
    	// System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV \n\n\n");



    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirtyTid = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return this.dirtyTid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int num_set_bits = 0;
        int j = 0;
        while (j < this.getHeaderSize()) {
           byte cur_header_val = header[j];
           for(int i = 0; i < 8; i++) {
              if ((cur_header_val & 1) == 1) {
                 num_set_bits++;
              }
              cur_header_val = (byte)(cur_header_val >> 1);
           }
           j++;
        }
        return this.numSlots - num_set_bits;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int whichByte =  i / 8;
        int bitOffset = i % 8;
        byte currByte = header[whichByte];
        currByte = (byte) (currByte >> bitOffset);
        return (currByte & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int whichByte = i / 8;
        int bitOffset = i % 8;
        byte currByte = header[whichByte];
        if (value) {
            currByte |= 1 << bitOffset;
        } else {
            currByte &= ~(1 << bitOffset);
        }
        header[whichByte] = currByte;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        ArrayList<Tuple> al = new ArrayList<>();
        for (int i=0;i<this.tuples.length;i++) {
            if (isSlotUsed(i)) {
                al.add(this.tuples[i]);
            }
        }
        return al.iterator();
    }
}

