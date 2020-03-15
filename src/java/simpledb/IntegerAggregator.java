package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Tuple>> map;
    private List<Tuple> list;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        map = new HashMap<>();
        list = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield != Aggregator.NO_GROUPING) {
            if (!map.containsKey(tup.getField(gbfield))) {
                map.put(tup.getField(gbfield), new ArrayList<>());
            }
            map.get(tup.getField(gbfield)).add(tup);
        } else {
            if (!map.containsKey(null)) {
                map.put(null, new ArrayList<>());
            }
            map.get(null).add(tup);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private boolean open = false;
            private TupleDesc tupleDesc;
            private Iterator<Map.Entry<Field, List<Tuple>>> entries;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                Type types[];
                if (gbfield != Aggregator.NO_GROUPING) {
                    types = new Type[2];
                    types[0] = gbfieldtype;
                    types[1] = Type.INT_TYPE;
                } else {
                    types = new Type[1];
                    types[0] = Type.INT_TYPE;
                }
                entries = map.entrySet().iterator();
                tupleDesc = new TupleDesc(types);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (open == false) return false;
                return entries.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (open == false) throw new IllegalStateException();
                Tuple tuple = new Tuple(tupleDesc);
                Map.Entry<Field, List<Tuple>> entry = null;
                if (entries.hasNext())
                    entry = entries.next();
                else
                    throw new NoSuchElementException();
                List<Tuple> tupleList = entry.getValue();
                int value = 0, k = -1;
                for (Tuple tp : tupleList) {
                    int v = ((IntField) tp.getField(afield)).getValue();
                    if (what == Op.SUM || what == Op.AVG) {
                        value += v;
                    } else if (what == Op.MIN) {
                        if (k < 0) value = v;
                        else value = Math.min(value, v);
                    } else if (what == Op.MAX) {
                        if (k < 0) value = v;
                        else value = Math.max(value, v);
                    } else if (what == Op.COUNT) {
                        value += 1;
                    }
                    k = 0;
                }
                if (what == Op.AVG) value = value / tupleList.size();
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(value));
                } else {
                    tuple.setField(0,  new IntField(value));
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                entries = map.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                open = false;
            }
        };
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    }

}
