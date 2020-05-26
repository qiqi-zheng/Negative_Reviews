import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    int count = 0;
    String line = value.toString();
    String[] str=line.split("\\|");

    if (str[3].equals("no")){
      count = count + 1;
    }

    context.write(new Text("Number of negative reviews for this customer is"), new IntWritable(count));

  }
}

