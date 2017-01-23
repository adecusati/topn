/**
 * TopN - Anthony DeCusati, 1/16/2017
 * given an arbitrarily large file and a number, N, containing individual numbers on each line (e.g. 200Gb file), will output the largest N numbers, highest first
 * Based on the WordCount example from https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {
    // The maximum number of records to output
    private static long max = -1;

    /**
     * Simple Mapper class that just writes out input data as keys for sorting
     */
    public static class SimpleMapper
            extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }

    /**
     * Simple Combiner class that will combine together the splits to complete sorting
     * All records need to be output here so the topN actually reflects the top N from
     * all records
     */
    public static class SimpleCombiner
            extends Reducer<Text,NullWritable,Text,NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    /**
     * This reducer class will only write out keys until the max is reached
     * It will write out all sorted records if no max was specified
     */
    public static class TopNReducer
            extends Reducer<Text,NullWritable,Text,NullWritable> {

        // Track how many records are written
        private long n = 0;

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // Check if this record should be written
            if( max < 0 || n++ < max ) {
                context.write(key, NullWritable.get());
            }
        }
    }

    /**
     * This comparator was pulled from http://tutorials.pcdhan.com/hadoop/sorting-mapreduce-key-in-descending-order/
     */
    public static class SortKeyComparator extends WritableComparator {

        public SortKeyComparator() {
            super(LongWritable.class, true);
        }

        /**
         * Need to implement our sorting mechanism.
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            LongWritable key1 = (LongWritable) a;
            LongWritable key2 = (LongWritable) b;

            // Implemet sorting in descending order
            int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
            return result;
        }
    }

    /**
     * Prints usage information.
     */
    public static void printUsage() {
        PrintStream stream = System.err;
        stream.println("Usage: topn INPUT OUTPUT [N]");
        stream.println();
        stream.println("\tINPUT");
        stream.println("\t\tSource data to sort");
        stream.println("\tOUTPUT");
        stream.println("\t\tDestination directory");
        stream.println("\t[N]");
        stream.println("\t\tOptional number of items to get");
        stream.println("\t\tDefault is all");
    }

    public static void main(String[] args) throws Exception {
        // Make sure enough arguments were provided
        // Print usage if not
        if( args.length < 2 ) {
            printUsage();
            System.exit(-1);
        } else if( args.length == 3 ) { // Set the max value if it was provided
            max = Long.parseLong(args[2]);
        }

        // Setup our job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TopN.class);
        job.setMapperClass(SimpleMapper.class);
        job.setCombinerClass(SimpleCombiner.class);
        job.setReducerClass(TopNReducer.class);
        job.setSortComparatorClass(SortKeyComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Input/output arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Run the sort
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}