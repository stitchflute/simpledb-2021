package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private ConcurrentHashMap<Field, Integer> group; // {hashcode: tuple}
    private int code; // used for no grouping

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
        this.op = what;
        this.group = new ConcurrentHashMap<>();
        this.code = 0;
        if(what != Op.COUNT)
            throw new IllegalArgumentException("op is not valid");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tup == null) {
            throw new NoSuchElementException("tup is not valid");
        }
        Field groupField = null;
        if (gbfield != NO_GROUPING) {
            groupField = tup.getField(gbfield);
        }
        else{
            groupField = new IntField(code);
        }
        // IntField a = (IntField)tup.getField(afield);
        if (group.containsKey(groupField)) {
            int val = group.get(groupField);
            group.put(groupField, val+1);
        }
        else {
            group.put(groupField, 1);
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
        return new StringAggregatorIterator();
        // throw new UnsupportedOperationException("please implement me for lab2");
    }

    public class StringAggregatorIterator implements OpIterator{
        // private IntegerAggregator ia;
        private TupleDesc td;
        private Iterator<Tuple> it;
        ArrayList<Tuple> tupleList;

        StringAggregatorIterator() {
            // this.ia = ia;
            if (gbfield == NO_GROUPING) {
                Type types[] = new Type[1];
                types[0] = gbfieldtype;
                td = new TupleDesc(types);
            }
            else {
                Type types[] = new Type[2];
                types[0] = gbfieldtype;
                types[1] = Type.INT_TYPE;
                td = new TupleDesc(types);
            }
            this.it = null;
            this.tupleList = new ArrayList<>();
        }
        /**
         * Opens the iterator. This must be called before any of the other methods.
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            Iterator<ConcurrentHashMap.Entry<Field, Integer>> entries = group.entrySet().iterator();
            while (entries.hasNext()) {
                ConcurrentHashMap.Entry<Field, Integer> entry = entries.next();
                int val = entry.getValue();
                Tuple t = new Tuple(td);
                if (gbfield == NO_GROUPING) {
                    t.setField(0, new IntField(val));
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1, new IntField(val));
                }
                tupleList.add(t);
            }
            it = tupleList.iterator();
        }

        /** Returns true if the iterator has more tuples.
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return (it != null && it.hasNext());
        }
        
        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        public Tuple next() throws DbException, TransactionAbortedException {
            if (hasNext()) {
                return it.next();
            }
            throw new NoSuchElementException("no element");
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Returns the TupleDesc associated with this OpIterator.
         * @return the TupleDesc associated with this OpIterator.
         */
        public TupleDesc getTupleDesc() {
            return this.td;
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        public void close() {
            tupleList.clear();
            it = null;
        }
            
    }

}
