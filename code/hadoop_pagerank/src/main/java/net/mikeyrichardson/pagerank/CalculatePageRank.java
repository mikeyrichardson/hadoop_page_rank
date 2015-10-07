package net.mikeyrichardson.pagerank;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.mikeyrichardson.pagerank.ConvergenceChecker.AbsoluteDifferenceCombiner;
import net.mikeyrichardson.pagerank.ConvergenceChecker.AbsoluteDifferenceReducer;
import net.mikeyrichardson.pagerank.TransitionMatrixCreator.TransitionMatrixReducer;
import net.mikeyrichardson.pagerank.io.BlockEntryKey;
import net.mikeyrichardson.pagerank.io.MatrixEntryWritable;
import net.mikeyrichardson.pagerank.TransitionMatrixCreator.BlockCalculationMapper;
import net.mikeyrichardson.pagerank.MultiplyMatrixAndVector.BlockMultiplicationReducer;
import net.mikeyrichardson.pagerank.MultiplyMatrixAndVector.RowPartitioner;
import net.mikeyrichardson.pagerank.MultiplyMatrixAndVector.VectorMapper;
import net.mikeyrichardson.pagerank.NormalizeVector.NormalizeMapper;


/**
 * This class copies a graph file to the HDFS file system, uses hadoop
 * to calculate the PageRank of each of the pages, and then copies the 
 * result back to the local file system. The input graph file should be a 
 * list of tab separated numbers with the first number respresenting
 * the source page and the second number representing the destination page. In
 * other words, the two numbers are the IDs of web pages and the first page 
 * contains a link to the second page.
 * @author Michael Richardson
 *
 */
public class CalculatePageRank extends Configured implements Tool {
    
    public static enum PageRankEnums { NUM_PAGES, VECTOR_SUM, ABS_DIFF_SUM };

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        File graphFile = new File(args[0]);
        File outputFile = new File(args[1]);
        if (!graphFile.exists()) {
            System.err.println("Input file does not exist");
            return 1;
        }
        
        // create the temp directories
        FileSystem fs = FileSystem.get(getConf());
        Path pathTmp = new Path("/tmp/tmp" + (int)(Math.random() * 10000000));
        while (fs.exists(pathTmp)) {
            pathTmp = new Path("/tmp/tmp" + (int)(Math.random() * 10000000));
        }
        fs.mkdirs(pathTmp);
        Path pathTmpGraph = new Path(pathTmp, "graph");
        Path pathTmpMatrix = new Path(pathTmp, "matrix");
        Path pathTmpVector = new Path(pathTmp, "vector");
        Path pathTmpResult = new Path(pathTmp, "result");
        Path pathTmpNormed = new Path(pathTmp, "normed");
        Path pathTmpAbsDiff = new Path(pathTmp, "absDiff");
        Path pathTmpOutput = new Path(pathTmp, "output");
        
        // web pages are represented by numbers in the graph file
        // find the largest number and add one to get the number
        // of pages since 0 is a page
//        long numPages = findMaxPage(graphFile) + 1;
        
        // Renumber the web pages so that they are consecutive, storing
        // the result for later lookup and then write the new
        // graph file to HDFS
        // This strategy can only be used with small web graphs. For graphs
        // too large to fit in memory, a database should be used.
        TwoWayLookUp<String> twoWayLookUp = createTwoWayLookUpAndWriteConsecutiveIdGraphFile(
                                                fs, graphFile, new Path(pathTmpGraph, "graph.txt"));
        long numPages = twoWayLookUp.size();
        getConf().set("map.pages.num", "" + numPages);
//        FileUtil.copy(graphFile, fs, pathTmpGraph, false, getConf());
        
        // write an initial vector file in SequenceFile format
        LongWritable key = new LongWritable();
        DoubleWritable value = new DoubleWritable(1.0 / numPages);
        SequenceFile.Writer sfWriter = SequenceFile.createWriter(fs, getConf(), 
                new Path(pathTmpVector, "vector.seq"), key.getClass(), value.getClass());
        for (long i = 0; i < numPages; i++) {
            key.set(i);
            sfWriter.append(key, value);
        }
        sfWriter.close();
        
        
        // create the sparse transition matrix in Sequence File format
        // the matrix is indexed in blocks
        int numDivs = getConf().getInt("map.divs.num", 2);
        double epsilon = getConf().getDouble("map.epsilon.value", 0.00001);
        
        Job jobTransitionMatrixCreation = Job.getInstance(getConf());
        jobTransitionMatrixCreation.setJarByClass(TransitionMatrixCreator.class);
        jobTransitionMatrixCreation.setInputFormatClass(KeyValueTextInputFormat.class);
        jobTransitionMatrixCreation.setOutputFormatClass(SequenceFileOutputFormat.class);

        jobTransitionMatrixCreation.setMapOutputKeyClass(LongWritable.class);
        jobTransitionMatrixCreation.setMapOutputValueClass(LongWritable.class);

        jobTransitionMatrixCreation.setOutputKeyClass(BlockEntryKey.class);
        jobTransitionMatrixCreation.setOutputValueClass(MatrixEntryWritable.class);

        jobTransitionMatrixCreation.setMapperClass(BlockCalculationMapper.class);
        jobTransitionMatrixCreation.setReducerClass(TransitionMatrixReducer.class);

        FileInputFormat.setInputPaths(jobTransitionMatrixCreation, pathTmpGraph);
        FileOutputFormat.setOutputPath(jobTransitionMatrixCreation, pathTmpMatrix);

        if (!jobTransitionMatrixCreation.waitForCompletion(true))
            return 1;

        while (true) {
            Job jobMatrixVectorMultiplication = Job.getInstance(getConf());
            jobMatrixVectorMultiplication.setJarByClass(MultiplyMatrixAndVector.class);
            MultipleInputs.addInputPath(jobMatrixVectorMultiplication, pathTmpMatrix,
                    SequenceFileInputFormat.class, Mapper.class);
            MultipleInputs.addInputPath(jobMatrixVectorMultiplication, pathTmpVector,
                    SequenceFileInputFormat.class, VectorMapper.class);
            jobMatrixVectorMultiplication.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setOutputPath(jobMatrixVectorMultiplication, pathTmpResult);
    
            jobMatrixVectorMultiplication.setMapOutputKeyClass(BlockEntryKey.class);
            jobMatrixVectorMultiplication.setMapOutputValueClass(MatrixEntryWritable.class);
    
            jobMatrixVectorMultiplication.setOutputKeyClass(LongWritable.class);
            jobMatrixVectorMultiplication.setOutputValueClass(DoubleWritable.class);
            jobMatrixVectorMultiplication.setPartitionerClass(RowPartitioner.class);
            jobMatrixVectorMultiplication.setReducerClass(BlockMultiplicationReducer.class);
            jobMatrixVectorMultiplication.setNumReduceTasks(numDivs);
            
            if (!jobMatrixVectorMultiplication.waitForCompletion(true))
                return 1;
            
            // use a counter from the matrix/vector multiplication job to store
            // the sum of the resulting vector entries
            long longVectorSum = jobMatrixVectorMultiplication.getCounters()
                                    .findCounter(PageRankEnums.VECTOR_SUM).getValue();
            double vectorSum = longVectorSum / MultiplyMatrixAndVector.DECIMAL_LONG_CONVERSION_FACTOR;
            getConf().set("map.vector.sum", "" + vectorSum);
            
            Job jobVectorNormalization = Job.getInstance(getConf());
            jobVectorNormalization.setJarByClass(NormalizeVector.class);
            jobVectorNormalization.setInputFormatClass(SequenceFileInputFormat.class);
            jobVectorNormalization.setOutputFormatClass(SequenceFileOutputFormat.class);
    
            jobVectorNormalization.setMapOutputKeyClass(LongWritable.class);
            jobVectorNormalization.setMapOutputValueClass(DoubleWritable.class);
    
            jobVectorNormalization.setOutputKeyClass(LongWritable.class);
            jobVectorNormalization.setOutputValueClass(DoubleWritable.class);
    
            jobVectorNormalization.setMapperClass(NormalizeMapper.class);
            jobVectorNormalization.setNumReduceTasks(0);
    
            FileInputFormat.setInputPaths(jobVectorNormalization, pathTmpResult);
            FileOutputFormat.setOutputPath(jobVectorNormalization, pathTmpNormed);
            if (!jobVectorNormalization.waitForCompletion(true)) 
                return 1;
    
            Job jobConvergenceChecking = Job.getInstance(getConf());
            jobConvergenceChecking.setJarByClass(ConvergenceChecker.class);
            jobConvergenceChecking.setInputFormatClass(SequenceFileInputFormat.class);
            jobConvergenceChecking.setOutputFormatClass(SequenceFileOutputFormat.class);
    
            jobConvergenceChecking.setMapOutputKeyClass(LongWritable.class);
            jobConvergenceChecking.setMapOutputValueClass(DoubleWritable.class);
    
            jobConvergenceChecking.setOutputKeyClass(LongWritable.class);
            jobConvergenceChecking.setOutputValueClass(DoubleWritable.class);
    
            jobConvergenceChecking.setCombinerClass(AbsoluteDifferenceCombiner.class);
            jobConvergenceChecking.setReducerClass(AbsoluteDifferenceReducer.class);
            FileInputFormat.setInputPaths(jobConvergenceChecking, pathTmpNormed);
            FileInputFormat.addInputPath(jobConvergenceChecking, pathTmpVector);
            FileOutputFormat.setOutputPath(jobConvergenceChecking, pathTmpAbsDiff);
            if (!jobConvergenceChecking.waitForCompletion(true)) 
                return 1;
            
            // use a counter to store the sum of the differences
            long absDiffSum = jobConvergenceChecking.getCounters()
                    .findCounter(PageRankEnums.ABS_DIFF_SUM).getValue();
            double sumDiffs = (absDiffSum / ConvergenceChecker.DECIMAL_LONG_CONVERSION_FACTOR);
            System.out.println("Sum of absolute differences: " + sumDiffs);
            if (sumDiffs < epsilon) {
                break;
            }
            
            // store the new vector result as the previous result and delete
            // directories before starting next iteration of loop
            fs.delete(pathTmpVector, true);
            FileUtil.copy(fs, pathTmpNormed, fs, pathTmpVector, true, true, getConf());
            fs.delete(pathTmpResult, true);
            fs.delete(pathTmpAbsDiff, true);
        }

        Job jobVectorReassembly = Job.getInstance(getConf());
        jobVectorReassembly.setInputFormatClass(SequenceFileInputFormat.class);
        jobVectorReassembly.setOutputFormatClass(TextOutputFormat.class);

        jobVectorReassembly.setMapOutputKeyClass(LongWritable.class);
        jobVectorReassembly.setMapOutputValueClass(DoubleWritable.class);

        jobVectorReassembly.setOutputKeyClass(LongWritable.class);
        jobVectorReassembly.setOutputValueClass(DoubleWritable.class);

        jobVectorReassembly.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(jobVectorReassembly, pathTmpNormed);
        FileOutputFormat.setOutputPath(jobVectorReassembly, pathTmpOutput);
        
        if (!jobVectorReassembly.waitForCompletion(true))
            return 1;
        
        // copy output to local file system and delete temp HDFS directories
        FileUtil.copy(fs, new Path(pathTmpOutput, "part-r-00000"), outputFile, false, getConf());
        
        Scanner scanner = new Scanner(fs.open(new Path(pathTmpOutput, "part-r-00000")));
        PrintWriter writer = new PrintWriter(outputFile);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] pair = line.split("\t");
            String origKey = twoWayLookUp.getKey(Integer.parseInt(pair[0]));
            writer.println(origKey + "\t" + pair[1]);
        }
        scanner.close();
        writer.close();
        
        fs.delete(pathTmp, true);
        return 0;
    }
    
    // Input the original page IDs into a two way look up and then re-write the original 
    // graph file to HDFS using the new consecutive page IDs
    private static TwoWayLookUp<String> createTwoWayLookUpAndWriteConsecutiveIdGraphFile(
            FileSystem fs, File in, Path outPath) throws FileNotFoundException, IOException {
        Scanner scanner = new Scanner(in);
        PrintWriter writer = new PrintWriter(fs.create(outPath));
        TwoWayLookUp<String> twlu = new TwoWayLookUp<String>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (line.startsWith("#"))
                continue;
            String[] pair = line.split("\t");
            int idSrc = twlu.getValue(pair[0]);
            int idDest = twlu.getValue(pair[1]);
            writer.println(idSrc + "\t" + idDest);
        }
        scanner.close();
        writer.close();
        return twlu;
    }


    private static class TwoWayLookUp<K extends Comparable<K>> {
        
        private TreeMap<K, Integer> keyToValueMap;
        private ArrayList<K> valueToKeyList;
        
        public TwoWayLookUp() {
            keyToValueMap = new TreeMap<K, Integer>();
            valueToKeyList = new ArrayList<K>();
        }

        public int getValue(K key) {
            if (this.keyToValueMap.containsKey(key))
                return this.keyToValueMap.get(key);
            valueToKeyList.add(key);
            this.keyToValueMap.put(key, valueToKeyList.size() - 1);
            return this.keyToValueMap.get(key);
        }
        
        public K getKey(int value) {
            return valueToKeyList.get(value);
        }
        
        public int size() {
            return valueToKeyList.size();
        }
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CalculatePageRank(), args);
    }

}