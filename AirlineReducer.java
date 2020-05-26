import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlineReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  private int total_count = 0;

  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

      for (IntWritable value : values) {
        this.total_count += value.get();
      } 

  }


  @Override
  public void cleanup(Context context) 
    throws IOException, InterruptedException{
     context.write(new Text("Total Number of negative reviews is"), new IntWritable(this.total_count));
  }

 

}