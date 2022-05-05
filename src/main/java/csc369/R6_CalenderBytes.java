package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class R6_CalenderBytes {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text full_date = new Text();

        String sa2 = sa[3].split(":")[0];
        String day = sa2.split("/")[0].substring(1);
        String month = sa2.split("/")[1];
        String year = sa2.split("/")[2];

        int int_bytes = Integer.parseInt(sa[9]);
        IntWritable bytes = new IntWritable(int_bytes); 

	    full_date.set(day + " " + month + " " + year);
	    context.write(full_date, bytes);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text full_date, Iterable<IntWritable> bytes, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = bytes.iterator();
        
            while (itr.hasNext()) {
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(full_date, result);
       }
    }

}
