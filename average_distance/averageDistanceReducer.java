import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class averageDistanceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
@Override public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	
	double total = 0.0;
	int count = 0;
	for (FloatWritable value : values) {

		total = total + value.get(); 
		count = count + 1;

	}

	double average_value = total/count;

	context.write(key, new FloatWritable( (float) average_value));
}
}
