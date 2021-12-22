package simpledb.execution;

import java.io.IOException;
// import java.lang.reflect.Type;
import java.util.Iterator;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator children[];
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private int numInsert;
    private Boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("Insert: td is not valid"); 
            
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.tid = t;
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.child = child;
        this.tableId = tableId;
        this.numInsert = 0;
        this.called = false;   
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // return null;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called)
            return null;
        this.called = true;
        while(child.hasNext()){
            Tuple t = child.next();
            try{
                Database.getBufferPool().insertTuple(tid, tableId, t);
                numInsert++;
            }
            catch(IOException e){
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple  = new Tuple(td);
        tuple.setField(0, new IntField(numInsert));
        return tuple;
        // return null;
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
