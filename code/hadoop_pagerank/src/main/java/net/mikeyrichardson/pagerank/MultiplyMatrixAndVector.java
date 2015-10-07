package net.mikeyrichardson.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import net.mikeyrichardson.pagerank.io.BlockEntryKey;
import net.mikeyrichardson.pagerank.io.MatrixEntryWritable;

import java.io.IOException;

/**
 * This class contains mapper and reducer classes to carry out matrix/vector multiplication
 * block by block and row by row. This allows matrix/vector multiplication even when
 * the matrix/vector are too large to fit in the memory of the machine.
 * @author Michael Richardson
 *
 */
public class MultiplyMatrixAndVector {
    
    final public static double DECIMAL_LONG_CONVERSION_FACTOR = 1e18;
 
    public static class VectorMapper extends
            Mapper<LongWritable, DoubleWritable, BlockEntryKey, MatrixEntryWritable> {
        
        private int numDivs = 0;
        private int numPages = 0;
        private int numPagesPerDiv = 0;
        private int numDivsWithOneExtraPage = 0;

        @Override
        public void setup(Context context) {
            this.numDivs = context.getConfiguration().getInt(
                    "map.divs.num", 2);
            this.numPages = context.getConfiguration().getInt(
                    "map.pages.num", 1000000);
            this.numPagesPerDiv = this.numPages / numDivs;
            this.numDivsWithOneExtraPage = this.numPages % this.numDivs;
        }
        
        // sends the 
        public void map(LongWritable key, DoubleWritable value, Context context) 
                throws IOException, InterruptedException {
            long page = key.get();
            double weight = value.get();
            int vectorDiv = 0, i = 0, j = 0;
            if (page < this.numDivsWithOneExtraPage * (numPagesPerDiv + 1)) {
                vectorDiv = (int) (page / (numPagesPerDiv + 1));
                i = (int) (page % (numPagesPerDiv + 1));
            }
            else {
                vectorDiv = (int) ((page - this.numDivsWithOneExtraPage) / numPagesPerDiv);
                i = (int) ((page - this.numDivsWithOneExtraPage) % numPagesPerDiv);
            }
            for (int blockRow = 0; blockRow < numDivs; blockRow++) {
                context.write(new BlockEntryKey(blockRow, vectorDiv, BlockEntryKey.VECTOR_SOURCE),
                          new MatrixEntryWritable(i, j, weight));
            }

        }
    }

    public static class BlockMultiplicationReducer extends
            Reducer<BlockEntryKey, MatrixEntryWritable, LongWritable, DoubleWritable> {
        
        private int numDivs = 0;
        private int numPages = 0;
        private int numPagesPerDiv = 0;
        private int numDivsWithOneExtraPage = 0;
        private double teleportationRate = 0.0;
        private int currentBlockRow = -1;
        private double[] vectorBlock;
        private double[] resultBlock;

        @Override
        public void setup(Context context) {
            this.teleportationRate = context.getConfiguration().getDouble(
                    "map.teleportation.rate", 0.15);
            this.numDivs = context.getConfiguration().getInt(
                    "map.divs.num", 2);
            this.numPages = context.getConfiguration().getInt(
                    "map.pages.num", 1000000);
            this.numPagesPerDiv = this.numPages / numDivs;
            this.numDivsWithOneExtraPage = this.numPages % this.numDivs;
            vectorBlock = new double[this.numPagesPerDiv + 1];
            resultBlock = new double[this.numPagesPerDiv + 1];
        }
        
        @Override
        public void reduce(BlockEntryKey key, Iterable<MatrixEntryWritable> values, Context context)
                throws IOException, InterruptedException {
            // when moving to a new row, write the results before zeroing the vector
            if (key.row.get() != currentBlockRow) {
                if (currentBlockRow != -1) {
                    writeResultBlock(context);
                }
                currentBlockRow = key.row.get();
                for (int row = 0; row < resultBlock.length; row++) {
                    vectorBlock[row] = 0;
                    resultBlock[row] = 0;
                }
            }
            if (key.source.get() == BlockEntryKey.VECTOR_SOURCE) {
                for (MatrixEntryWritable entry : values) {
                   vectorBlock[entry.row.get()] = entry.value.get();
                }
            }
            else {
                for (MatrixEntryWritable entry : values) {
                    resultBlock[entry.row.get()] += entry.value.get() * vectorBlock[entry.col.get()];
                }
            }
        }
        
        @Override
        public void cleanup(Context context) throws InterruptedException, IOException {
            writeResultBlock(context);
        }
        
        private void writeResultBlock(Context context) throws IOException, InterruptedException {
            int blockLength = this.numPagesPerDiv;
            if (currentBlockRow < this.numDivsWithOneExtraPage) {
                blockLength++;
            }
            double entriesSum = 0.0;
            long rowOffset = 0;
            if (currentBlockRow < this.numDivsWithOneExtraPage) {
                rowOffset = ((long) numPagesPerDiv + 1) * currentBlockRow;
            }
            else {
                rowOffset = ((long) numPagesPerDiv + 1) * this.numDivsWithOneExtraPage +
                        ((long) numPagesPerDiv) * (currentBlockRow - this.numDivsWithOneExtraPage);
            }
            for (int row = 0; row < blockLength; row++) {
                double product = (1 - this.teleportationRate) * resultBlock[row];
                entriesSum += product;
                context.write(new LongWritable(rowOffset + row), 
                        new DoubleWritable(product));
            }
            long longSum = (long) (DECIMAL_LONG_CONVERSION_FACTOR * entriesSum); 
            context.getCounter(CalculatePageRank.PageRankEnums.VECTOR_SUM).increment(longSum);
        }
    }
    
    public static class RowPartitioner extends Partitioner<BlockEntryKey, MatrixEntryWritable> {
        
        @Override
        public int getPartition(BlockEntryKey key, MatrixEntryWritable value, int numPartitions) {
            return key.row.get() % numPartitions;
        }
    }

}