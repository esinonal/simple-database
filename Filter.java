package simpledb;

import java.util.*; 

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator tuplesToFilter;
    private Predicate predicate;
    private TupleDesc tupleDesc;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        super();
        this.tuplesToFilter = child;
        this.predicate = p;
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.tuplesToFilter.open();
        super.open();
    }

    public void close() {
        super.close();
        this.tuplesToFilter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.tuplesToFilter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {

        Tuple next;
        while (this.tuplesToFilter.hasNext()) {
            next = this.tuplesToFilter.next();
            if (this.predicate.filter(next)) {
                return next;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren()  {
        return new OpIterator[] { this.tuplesToFilter };
    }

    @Override
    public void setChildren(OpIterator[] children)  {
        this.tuplesToFilter = children[0];
    }
}
