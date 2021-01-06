package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = this.getLeftPageIterator();
            fetchNextLeftBlock();

            this.rightIterator = this.getRightPageIterator();
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            if (!leftIterator.hasNext()) {
                // left table all used up case
                leftRecordIterator = null;
                leftRecord = null;
            }
            else {
                // fetch B-2 pages and construct the left record iterator
                leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator,
                        numBuffers - 2);
                // mark its first position
                leftRecordIterator.markNext();
                // set the left record to be the first record in the block
                leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            if (!rightIterator.hasNext()) {
                // right table all used up case
                rightRecordIterator = null;
            }
            else {
                rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
                // mark its first position
                rightRecordIterator.markNext();
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement
            nextRecord = null;
            while (!hasNext()) {
                // loop until get the next record
                // throw NoSuchElementException when no more pair to inspect
                if (leftRecord == null || rightRecordIterator == null) {
                    // all complete case
                    throw new NoSuchElementException("No new record to fetch");
                }
                if (rightRecordIterator.hasNext()) {
                    // Case 1: The right iterator has a value to yield
                    // get one right record
                    Record rightRecord = rightRecordIterator.next();
                    // yield the output after comparison
                    DataBox leftJoinValue = leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        nextRecord = joinRecords(leftRecord, rightRecord);
                    }
                }
                else if (leftRecordIterator.hasNext()) {
                    // Case 2: The right iterator doesn't have a value to yield but the left iterator does
                    // one step in the left block and restart the same right page
                    leftRecord = leftRecordIterator.next();
                    rightRecordIterator.reset();
                }
                else {
                    // will only have case 3 or 4
                    rightRecordIterator = null;
                    if (rightIterator.hasNext()) {
                        // try to get next right page
                        fetchNextRightPage();
                    }
                    if (rightRecordIterator != null) {
                        // Case 3: Neither the right nor left iterators have values to yield, but there's more right pages
                        // reset left block to its start and fetch the next left record
                        leftRecordIterator.reset();
                        leftRecord = leftRecordIterator.next();
                    }
                    else if (leftIterator.hasNext()){
                        // Case 4: Neither right nor left iterators have values nor are there more right pages, but there are still left blocks
                        // get the next left block, reset right page iterator to the first right page
                        fetchNextLeftBlock();
                        rightIterator.reset();
                        fetchNextRightPage();
                        if (leftRecordIterator == null || rightRecordIterator == null) {
                            throw new NoSuchElementException("No new record to fetch");
                        }
                    }
                    else {
                        throw new NoSuchElementException("No new record to fetch");
                    }
                }
            }
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
