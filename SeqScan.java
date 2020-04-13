package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private DbFile f;
    private TupleDesc td;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        if (tableAlias == null) {
            this.tableAlias = "null";
        } else {
            this.tableAlias = tableAlias;
        }
        this.f = Database.getCatalog().getDatabaseFile(this.tableId);
        this.iterator = this.f.iterator(tid);
        this.td = this.makeTupleDesc();
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        if (tableAlias == null) {
            this.tableAlias = "null";
        } else {
            this.tableAlias = tableAlias;
        }
        this.td = this.makeTupleDesc();
    }

    public SeqScan(TransactionId tid, int tableId) {

        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.iterator.open();
    }

    private TupleDesc makeTupleDesc() {
        TupleDesc desc = this.f.getTupleDesc();
        Iterator<TupleDesc.TDItem> ittd = desc.iterator();
        int numFields = desc.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        for(int i=0;i<numFields;i++) {
            assert ittd.hasNext();
            TupleDesc.TDItem tdItem = ittd.next();
            typeAr[i] = tdItem.fieldType;
            fieldAr[i] = this.tableAlias + "." + tdItem.fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        try {
            return this.iterator.next();
        } catch (NoSuchElementException e) {
            throw new DbException(e.getMessage());
        }
    }

    public void close() {
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.iterator.rewind();
    }
}
