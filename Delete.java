package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int flag;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        this.flag = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{"D"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    private int deleteAndCount() throws DbException, TransactionAbortedException {
        int counter = 0;
        while (child.hasNext()) {
            try {
                counter += 1;
                Tuple n = child.next();
                Database.getBufferPool().deleteTuple(this.t, n);
            } catch (IOException ex) {
                throw new DbException("can't delete");
            }
        }
        return counter;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.open) {
            return null;
        }
        // if called more than once
        if (this.flag == 1) {
            return null;
        }
        flag = 1;
        int count = deleteAndCount();
        Tuple info = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"D"}));
        info.setField(0, new IntField(count));
        return info;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
