package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AscendingSort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\\s+");
	    Text identifier = new Text();
	    identifier.set(sa[0]);

        int int_numsort = Integer.parseInt(sa[1]);
        IntWritable numsort = new IntWritable(int_numsort);
	    context.write(identifier, numsort);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        @Override
	protected void reduce(Text identifier, Iterable<IntWritable> numsort, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> itr = numsort.iterator();
        
            while (itr.hasNext()) {
                 context.write(identifier, itr.next());
            }
       }
    }

}