package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator children[];
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private TupleDesc td;
    private Aggregator aggr;
    private OpIterator it;
    private Type gbFieldType;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.td = getTupleDesc();
        this.it = null;
        this.gbFieldType = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
        // return -1;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if(gfield == Aggregator.NO_GROUPING)
            return null;
        return child.getTupleDesc().getFieldName(gfield);
        // return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
        // return -1;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
        // return null;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
        // return null;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        Type aftype = child.getTupleDesc().getFieldType(afield);
        if (aftype == Type.INT_TYPE) {
            aggr = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        }
        else
            aggr = new StringAggregator(gfield, gbFieldType, afield, aop);
        child.open();
        while (child.hasNext()) {
            aggr.mergeTupleIntoGroup(child.next());
        }
        child.close();
        it = aggr.iterator();
        it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // System.out.println("fetchnext: ");
        if (it != null && it.hasNext()) {
            Tuple t = it.next();
            // System.out.println("ttt: " + t.toString());
            return t;
        }
        return null;
        // return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    // some code goes here
        TupleDesc child_td = child.getTupleDesc();
        if (this.gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE},
                                 new String[]{aop.toString() + "(" + child_td.getFieldName(afield) + ")"});
        } else {
            return new TupleDesc(new Type[]{child_td.getFieldType(gfield), Type.INT_TYPE},
                                 new String[]{child_td.getFieldName(gfield), aop.toString() + "(" + child_td.getFieldName(afield) + ")"});
        }
    }

    public void close() {
        // some code goes here
        aggr = null;
        it = null;
        super.close();
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
