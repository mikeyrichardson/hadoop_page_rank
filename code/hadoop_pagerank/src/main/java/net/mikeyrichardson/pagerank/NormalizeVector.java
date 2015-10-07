package net.mikeyrichardson.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class contains a mapper to normalize a vector so that it sums to 1.
 * It must have the property "map.vector.sum" set correctly in order to work.
 * @author Michael Richardson
 *
 */
public class NormalizeVector {
    
    public static class NormalizeMapper extends
            Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

        private double vectorSum = 0.0;
        private long numPages = 0;
        private double normAmount = 0.0;

        @Override
        public void setup(Context context) {
            this.vectorSum = context.getConfiguration().getDouble("map.vector.sum", 0.0);
            this.numPages = context.getConfiguration().getLong("map.pages.num", 1000000);
            this.normAmount = (1 - this.vectorSum) / this.numPages;
        }
        
        public void map(LongWritable key, DoubleWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, new DoubleWritable(value.get() + this.normAmount));
        }
    }

}