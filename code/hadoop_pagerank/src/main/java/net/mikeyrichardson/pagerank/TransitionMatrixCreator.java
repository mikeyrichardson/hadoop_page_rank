package net.mikeyrichardson.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import net.mikeyrichardson.pagerank.io.BlockEntryKey;
import net.mikeyrichardson.pagerank.io.MatrixEntryWritable;

import java.io.IOException;
import java.util.ArrayList;

public class TransitionMatrixCreator {

    public static class BlockCalculationMapper extends
            Mapper<Text, Text, LongWritable, LongWritable> {
        


        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.toString().startsWith("#")) {
                return;
            }
            long srcPage = Long.parseLong(key.toString());
            long destPage = Long.parseLong(value.toString());
            context.write(new LongWritable(srcPage), new LongWritable(destPage));
            
        }
    }

    /**
     * This reducer calculates the transition probabilities from each page and 
     * also assigns a block key to each matrix entry. This allows matrix/vector multiplication
     * to be carried out when the matrix and/or vector are too large to fit in the memory
     * of a single machine.
     * @author Michael Richardson
     *
     */
    public static class TransitionMatrixReducer extends
            Reducer<LongWritable, LongWritable, BlockEntryKey, MatrixEntryWritable> {
        
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
            this.numPagesPerDiv = this.numPages / this.numDivs;
            this.numDivsWithOneExtraPage = this.numPages % this.numDivs;
        }
        
        // Most of the logic in this method accounts for unequal block sizes
        // that are necessary when the number of blocks doesn't divide 
        // evenly into the number of pages.
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long srcPage = key.get();
            ArrayList<Long> destPageList = new ArrayList<Long>();
            for (LongWritable value : values) {
                destPageList.add(value.get());
            }
            
            for (Long destPage : destPageList) {
                int blockRow = 0, blockCol = 0, entryRow = 0, entryCol = 0;
                if (destPage < this.numDivsWithOneExtraPage * (numPagesPerDiv + 1)) {
                    blockRow = (int) (destPage / (numPagesPerDiv + 1));
                    entryRow = (int) (destPage % (numPagesPerDiv + 1));
                }
                else {
                    blockRow = (int) ((destPage  - this.numDivsWithOneExtraPage) / numPagesPerDiv);
                    entryRow = (int) ((destPage - this.numDivsWithOneExtraPage) % numPagesPerDiv);
                }
                if (srcPage < this.numDivsWithOneExtraPage * (numPagesPerDiv + 1)) {
                    blockCol = (int) (srcPage / (numPagesPerDiv + 1));
                    entryCol = (int) (srcPage % (numPagesPerDiv + 1));
                }
                else {
                    blockCol = (int) ((srcPage - this.numDivsWithOneExtraPage) / numPagesPerDiv);
                    entryCol = (int) ((srcPage - this.numDivsWithOneExtraPage) % numPagesPerDiv);
                }
                double transitionProbability = 1.0 / destPageList.size();
                context.write(new BlockEntryKey(blockRow, blockCol, BlockEntryKey.MATRIX_SOURCE),
                              new MatrixEntryWritable(entryRow, entryCol, transitionProbability));
            }
        }
    }

}