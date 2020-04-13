package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    public ArrayList<Field> al = null;
    private TupleDesc tuple_desc = null;
    private RecordId recid = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        int numfields = td.numFields();
        assert numfields > 0;
        al = new ArrayList<Field>();
        for(int i = 0; i < numfields; i++) {
            al.add(null);
	    }
        this.tuple_desc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tuple_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        if (tuple_desc.numFields() > 0) {
            return recid;
        } else {
            return null;
        }
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        al.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return al.get(i);
        } catch(NoSuchElementException ex) {
            return null;
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String contents = "";
        for(int i = 0; i < al.size(); i++) {
            Field field_obj = al.get(i);
            contents += field_obj.toString() + "\t";
        }
        return contents;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        Iterator<Field> itr = al.iterator();
        return itr;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tuple_desc = td;
    }

    public static Tuple mergeTuples(TupleDesc td, Tuple tuple1, Tuple tuple2) {
        Tuple mergedTuple = new Tuple(td);
        int sizeTuple1 = tuple1.getTupleDesc().numFields();
        int sizeTuple2 = tuple2.getTupleDesc().numFields();
        int index = 0;
        assert td.numFields() == sizeTuple1 + sizeTuple2;
        while (index != sizeTuple1) {
            mergedTuple.setField(index, tuple1.getField(index));
            index++;
        }
        index = 0;
        while (index != sizeTuple2) {
            mergedTuple.setField(sizeTuple1 + index, tuple2.getField(index));
            index++;
        }
        return mergedTuple;
    }
}
