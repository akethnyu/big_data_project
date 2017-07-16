import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class averageDistanceMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
{
	@Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		int distance_record = 4;
		String record_string = value.toString();
		String[] record_fields = record_string.split(",");
		
		float distance = Float.parseFloat(record_fields[distance_record]);
		context.write( new Text ("average_distance"), new FloatWritable(distance) );		

	}
}
