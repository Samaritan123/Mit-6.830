package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Tuple>> map;
    private List<Tuple> list;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) throw new IllegalArgumentException();
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        map = new HashMap<>();
        list = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield != Aggregator.NO_GROUPING) {
            if (!map.containsKey(tup.getField(gbfield))) {
                map.put(tup.getField(gbfield), new ArrayList<>());
            }
            map.get(tup.getField(gbfield)).add(tup);
        } else {
            list.add(tup);
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
        return new OpIterator() {
            private boolean open = false;
            private TupleDesc tupleDesc;
            private Iterator<Map.Entry<Field, List<Tuple>>> entries;
            private Iterator<Tuple> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                Type types[];
                if (gbfield != Aggregator.NO_GROUPING) {
                    types = new Type[2];
                    types[0] = gbfieldtype;
                    types[1] = Type.INT_TYPE;
                    entries = map.entrySet().iterator();
                } else {
                    types = new Type[1];
                    types[0] = Type.INT_TYPE;
                    iterator = list.iterator();
                }
                tupleDesc = new TupleDesc(types);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (open == false) return false;
                if (gbfield != Aggregator.NO_GROUPING)
                    return entries.hasNext();
                else
                    return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (open == false) return null;
                int value = 0, k = -1;
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    Map.Entry<Field, List<Tuple>> entry = null;
                    if (entries.hasNext())
                        entry = entries.next();
                    else
                        throw new NoSuchElementException();
                    List<Tuple> tupleList = entry.getValue();
                    value = tupleList.size();
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(value));
                } else {
                    Tuple tp = null;
                    if (iterator.hasNext())
                        tp = iterator.next();
                    else
                        throw new NoSuchElementException();
                    tuple.setField(0,  ((IntField) tp.getField(afield)));
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                entries = map.entrySet().iterator();
                iterator = list.iterator();
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
    }

}
