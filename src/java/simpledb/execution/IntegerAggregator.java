package simpledb.execution;

import javax.lang.model.element.TypeElement;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private class Group {
        public Field key; // group field
        public double result;
        // for avg
        public int count;
        public int sum;

        public Group(Field key) {
            this.key = key;
            this.result = 1;
            this.count = 1;
            this.sum = 0;
        }
    }

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private ConcurrentHashMap<Integer, ArrayList<Group>> group; // {hashcode: tuple}
    private int code; // used for no grouping

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
        // System.out.println("IntegerAggregator");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.group = new ConcurrentHashMap<>();
        this.code = 0;
    }

    public Group initGroup(Field f, int val) {
        Group g = new Group(f);
        switch (op) {
            case SUM:
                g.result = val;
                break;
            case COUNT:
                g.result = 1;
                break;
            case MAX:
                g.result = val;
                break;
            case MIN:
                g.result = val;
                break;
            case AVG:
                g.count = 1;
                g.sum = val;
                g.result = g.sum * 1.0 / g.count;
                break;
            default:
                ;
        }
        return g;
    }

    public Group work(Group g, int val) {
        switch (op) {
            case SUM:
                g.result += val;
                break;
            case COUNT:
                g.result += 1;
                break;
            case MAX:
                g.result = Math.max(g.result, val);
                break;
            case MIN:
                g.result = Math.min(g.result, val);
                break;
            case AVG:
                g.count += 1;
                g.sum += val;
                g.result = g.sum * 1.0 / g.count;
                break;
            default:
                return null;
        }
        return g;
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
        // System.out.println("merge");
        if (tup == null) {
            throw new NoSuchElementException("tup is not valid");
        }
        int key;
        Field gfield = null;
        if (gbfield != NO_GROUPING) {
            gfield = tup.getField(gbfield);
            key = gfield.hashCode();
        }
        else{
            gfield = new IntField(code);
            key = code;
            // code++;
        }
        IntField a = (IntField)tup.getField(afield);
        if (group.containsKey(key)) {
            ArrayList<Group> list = group.get(key);
            Group g = null;
            int i;
            for (i = 0; i < list.size(); ++i) {
                g = list.get(i);
                if(g.key.equals(gfield)){
                    Group ng = work(g, a.getValue());
                    list.set(i, ng);
                    break;
                }
            }
            if(i == list.size()){
                g = initGroup(gfield, a.getValue());
                list.add(g);
            }
            group.put(key, list);
        }
        else {
            Group g = initGroup(gfield, a.getValue());
            ArrayList<Group> list = new ArrayList<>();
            list.add(g);
            group.put(key, list);
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
        // System.out.println("iterator");
        return new IntegerAggregatorIterator();
        // throw new UnsupportedOperationException("please implement me for lab2");
    }
    
    public class IntegerAggregatorIterator implements OpIterator{
        // private IntegerAggregator ia;
        private TupleDesc td;
        private Iterator<Tuple> it;
        ArrayList<Tuple> tupleList;

        IntegerAggregatorIterator() {
            // this.ia = ia;
            if (gbfield == NO_GROUPING) {
                Type types[] = new Type[1];
                types[0] = Type.INT_TYPE;
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
            Iterator<ConcurrentHashMap.Entry<Integer, ArrayList<Group>>> entries = group.entrySet().iterator();
            while (entries.hasNext()) {
                ConcurrentHashMap.Entry<Integer, ArrayList<Group>> entry = entries.next();
                ArrayList<Group> list = entry.getValue();
                for (int i = 0; i < list.size(); ++i) {
                    Group g = list.get(i);
                    int val = (int) (g.result);
                    Tuple t = new Tuple(td);
                    if (gbfield == NO_GROUPING) {
                        t.setField(0, new IntField(val));
                    } else {
                        t.setField(0, g.key);
                        t.setField(1, new IntField(val));
                    }
                    tupleList.add(t);
                }
            }
            // for(Tuple t: tupleList){
            //     System.out.println("t: " + t.toString());
            // }
            // System.out.println("size: " + tupleList.size());
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
