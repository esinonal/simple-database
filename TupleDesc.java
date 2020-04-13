package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> al = null;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> itr = al.iterator();
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr.length > 0;
        assert fieldAr.length > 0;
        al = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdi = new TDItem(typeAr[i], fieldAr[i]); 
            al.add(tdi);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        assert typeAr.length > 0;
        al = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdi = new TDItem(typeAr[i], null); 
            al.add(tdi);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return al.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            TDItem tdi = al.get(i);
            return tdi.fieldName;
        } catch(NoSuchElementException ex) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            TDItem tdi = al.get(i);
            return tdi.fieldType;
        } catch(NoSuchElementException ex) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        try {
            for (int i = 0; i < al.size(); i++) {
                TDItem tdi = al.get(i);
                if (tdi.fieldName.equals(name)) {
                    return i;
                }
            }
        } catch(Exception ex) {
            throw new NoSuchElementException();
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total_size = 0;
        for(int i = 0; i < al.size(); i++) {
          total_size += al.get(i).fieldType.getLen();
        }
            return total_size;
        }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] new_type_ar = new Type[td1.numFields() + td2.numFields()];
        String[] new_name_ar = new String[td1.numFields() + td2.numFields()];
        for(int i = 0; i < td1.numFields(); i++) {
          new_type_ar[i] = td1.getFieldType(i);
          new_name_ar[i] = td1.getFieldName(i);
        }
        int next_ind = td1.numFields();
        for(int j = 0; j < td2.numFields(); j++) {
          new_type_ar[j + next_ind] = td2.getFieldType(j);
          new_name_ar[j + next_ind] = td2.getFieldName(j);
        }
        TupleDesc new_tuple_desc = new TupleDesc(new_type_ar, new_name_ar);
        return new_tuple_desc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
          return true;
        }
        if (!(o instanceof TupleDesc)) {
          return false;
        }
        TupleDesc o_td = (TupleDesc) o;
        if (al.size() == o_td.numFields()) {
          for(int i = 0; i < al.size(); i++) {
            if (al.get(i).fieldType != o_td.getFieldType(i)) {
              return false;
            }
          }
              return true;
        } else {
          return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String desc_str = "";
        for(int i = 0; i < al.size(); i++) {
            TDItem tdi = al.get(i);
            desc_str += tdi.toString() + ", ";  
        }
        return desc_str;
    }
}
