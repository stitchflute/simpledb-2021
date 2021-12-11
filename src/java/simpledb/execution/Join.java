package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator children[];
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t1;
    private Tuple t2;
    private Tuple t; // new tuple
    private int len1;
    private int len2;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.children = new OpIterator[2];
        this.children[0] = child1;
        this.children[1] = child2;
        this.child1 = child1;
        this.child2 = child2;
        this.t1 = null;
        this.t2 = null;
        this.len1 = child1.getTupleDesc().numFields();
        this.len2 = child2.getTupleDesc().numFields();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
        // return null;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
        // return null;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
        // return null;
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        return td;
        // return null;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        t = new Tuple(getTupleDesc());
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // child1.rewind();
        // child2.rewind();
        close();
        open();
        t1 = null;
        t2 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(t1 != null || child1.hasNext()){
            if(t1 == null)
                t1 = child1.next();
            while(child2.hasNext()){
                t2 = child2.next();
                if(p.filter(t1, t2)){
                    for(int i = 0; i < len1; ++i){
                        t.setField(i, t1.getField(i));
                    }
                    for(int i = 0; i < len2; ++i){
                        t.setField(i+len1, t2.getField(i));
                    }
                    return t;
                }
            }
            t1 = null;
            child2.rewind();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
        // return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
