package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator children[];
    private OpIterator child;
    private TupleDesc td;
    private int numDelete;
    private Boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.tid = t;
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.child = child;
        this.numDelete = 0;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // return null;
        if(this.called)
            return null;
        this.called = true;
        while(child.hasNext()){
            Tuple t = child.next();
            try{
                Database.getBufferPool().deleteTuple(tid, t);
                numDelete++;
            }
            catch(IOException e){
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple  = new Tuple(td);
        tuple.setField(0, new IntField(numDelete));
        return tuple;
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
