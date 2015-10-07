package net.mikeyrichardson.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This class contains a reducer to find the sum of absolute differences
 * between the entries of two vectors. 
 * @author Michael Richardson
 *
 */
public class ConvergenceChecker {
    
    final public static double DECIMAL_LONG_CONVERSION_FACTOR = 1e8;

    public static class AbsoluteDifferenceCombiner
            extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sign = 1;
            double diff = 0.0;
            for (DoubleWritable v : values) {
                diff += sign * v.get();
                sign *= -1;
            }
            context.write(key, new DoubleWritable(Math.abs(diff)));
        }
    }  
    
    public static class AbsoluteDifferenceReducer extends
            Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
        
        private double absDiffSum = 0.0;
        
        @Override
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sign = 1;
            double diff = 0.0;
            for (DoubleWritable v : values) {
                diff += sign * v.get();
                sign *= -1;
            }
            this.absDiffSum += Math.abs(diff);
            context.write(key, new DoubleWritable(Math.abs(diff)));
        }
        
        @Override
        public void cleanup(Context context) {
            long sum = (long) (DECIMAL_LONG_CONVERSION_FACTOR * this.absDiffSum);
            context.getCounter(CalculatePageRank.PageRankEnums.ABS_DIFF_SUM).increment(sum);
        }
        
    }

}