package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// test comment 4

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	} else if ("R1_URLCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R1_URLCount.ReducerImpl.class);
	    job.setMapperClass(R1_URLCount.MapperImpl.class);
	    job.setOutputKeyClass(R1_URLCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R1_URLCount.OUTPUT_VALUE_CLASS);
	} else if ("R2_HTTPCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R2_HTTPCount.ReducerImpl.class);
	    job.setMapperClass(R2_HTTPCount.MapperImpl.class);
	    job.setOutputKeyClass(R2_HTTPCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R2_HTTPCount.OUTPUT_VALUE_CLASS);
	} else if ("R3_HostnameBytes".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R3_HostnameBytes.ReducerImpl.class);
	    job.setMapperClass(R3_HostnameBytes.MapperImpl.class);
	    job.setOutputKeyClass(R3_HostnameBytes.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R3_HostnameBytes.OUTPUT_VALUE_CLASS);
	} else if ("R4_URLClientCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R4_URLClientCount.ReducerImpl.class);
	    job.setMapperClass(R4_URLClientCount.MapperImpl.class);
	    job.setOutputKeyClass(R4_URLClientCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R4_URLClientCount.OUTPUT_VALUE_CLASS);
	} else if ("R5_MonthYearCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R5_MonthYearCount.ReducerImpl.class);
	    job.setMapperClass(R5_MonthYearCount.MapperImpl.class);
	    job.setOutputKeyClass(R5_MonthYearCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R5_MonthYearCount.OUTPUT_VALUE_CLASS);
	} else if ("R6_CalenderBytes".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(R6_CalenderBytes.ReducerImpl.class);
	    job.setMapperClass(R6_CalenderBytes.MapperImpl.class);
	    job.setOutputKeyClass(R6_CalenderBytes.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(R6_CalenderBytes.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
