package simpledb;

import java.util.*;
import java.lang.*;
import java.util.concurrent.ConcurrentHashMap;

import static simpledb.Aggregator.Op.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, ArrayList<String>> group_agg_map;
    private TupleDesc td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.group_agg_map = new HashMap<>();

        if (gbfield == Aggregator.NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field tupgbField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            tupgbField = null;
        } else {
            tupgbField = tup.getField(this.gbfield);
        }
        if (!group_agg_map.containsKey(tupgbField))
        {
            group_agg_map.put(tupgbField, new ArrayList<String>());
        }
        group_agg_map.get(tupgbField).add(((StringField) tup.getField(this.afield)).getValue());
    }

    // compute aggregate value from stored integer list depending on what the given operator is
    private Integer computeAgg(ArrayList<String> aggs) throws Exception {
        if (aggs.size() == 0) {
            return null;
        }
        if (this.what == COUNT) {
            return aggs.size();
        } else {
            throw new Exception("operator not found");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // initialize tuple descriptor based on if grouping or not
        // populate each tuple list with group by fields (if yes) and associated computed aggregate values
        ArrayList<Tuple> tuple_list = new ArrayList<Tuple>();
        for (Map.Entry<Field, ArrayList<String>> entry : group_agg_map.entrySet()) {
            Integer agg = null;
            try {
                agg = computeAgg(entry.getValue());
            } catch (Exception e){}
            IntField agg_ifield = new IntField(new Integer(agg));
            Tuple cur_tuple = new Tuple(this.td);
            // set necessary tuple fields and append to list
            if (gbfield == Aggregator.NO_GROUPING) {
                cur_tuple.setField(0, agg_ifield);
                tuple_list.add(cur_tuple);
            } else {
                cur_tuple.setField(0, entry.getKey());
                cur_tuple.setField(1, agg_ifield);
                tuple_list.add(cur_tuple);
            }
        }
        return new TupleIterator(this.td, tuple_list);
    }

}
