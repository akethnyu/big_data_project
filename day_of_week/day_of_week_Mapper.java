import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Date;

public class day_of_week_Mapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		int DateAndTimeId = 1;
		String record_string = value.toString();
		String[] records = record_string.split(",");
		
		if ( !( records[1].equals("tpep_pickup_datetime") ) )
		{		
			String DateAndTimeRecord = records[1];
			String[] DateAndTimeField = DateAndTimeRecord.split("\\s+");
			String Full_Date = DateAndTimeField[0];
		
			String[] FullDateArray = Full_Date.split("-");
			int year = Integer.parseInt(FullDateArray[0]);
			int month = Integer.parseInt(FullDateArray[1]) - 1;
			int day = Integer.parseInt(FullDateArray[2]) - 1;

			Date date = new Date(year,month,day);
 			SimpleDateFormat simpleDateformat = new SimpleDateFormat("EEEE");
    		
			String DayOfTheWeek = simpleDateformat.format(date).toString();
		
			String OutputKey = record_string + "," + DayOfTheWeek; 		

			context.write( new Text(OutputKey), new Text("") );
		}

	}
}
