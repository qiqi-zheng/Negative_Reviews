import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int comfort_score = 0;
        int staff_service = 0;
        int food_bev = 0;
        int ground_service = 0;
        int inflight_ent = 0;
        int wifi = 0;
        int money = 0;

        int total_comfort = 0;
        int total_staff = 0;
        int total_food = 0;
        int total_ground = 0;
        int total_inflight = 0;
        int total_wifi = 0;
        int total_money = 0;

        String[] line = value.toString().toLowerCase().split("\\n");

        for (int i = 0; i < line.length; i++) {

            String[] str = line[i].split("\\|");

            for (int j = 0; j < str.length; j++) {

                if (str[3].equals("no")) {

                    if (str[4].equals("1")) {
                        comfort_score = Integer.parseInt(str[4]);
                    }
                    if (str[5].equals("1")) {
                        staff_service = Integer.parseInt(str[5]);
                    }
                    if (str[6].equals("1")) {
                        food_bev = Integer.parseInt(str[6]);
                    }
                    if (str[7].equals("1")) {
                        ground_service = Integer.parseInt(str[7]);
                    }
                    if (str[8].equals("1")) {
                        inflight_ent = Integer.parseInt(str[8]);
                    }
                    if (str[9].equals("1")) {
                        wifi = Integer.parseInt(str[9]);
                    }
                    if (str[10].contentEquals("1")) {
                        money = Integer.parseInt(str[10]);
                    }
                }

            }
            total_comfort = total_comfort + comfort_score;
            total_staff = total_staff + staff_service;
            total_food = total_food + food_bev;
            total_ground = total_ground + ground_service;
            total_inflight = total_inflight + inflight_ent;
            total_wifi = total_wifi + wifi;
            total_money = total_money + money;
        }
		context.write(new Text("one rating for seat comfort"), new IntWritable(total_comfort));
		context.write(new Text("one rating for staff service"), new IntWritable(total_staff));
		context.write(new Text("one rating for food and beverage"), new IntWritable(total_food));
		context.write(new Text("one rating for ground service"), new IntWritable(total_ground));
		context.write(new Text("one rating for inflight entertainment"), new IntWritable(total_inflight));
		context.write(new Text("one rating for wifi inflight"), new IntWritable(total_wifi));
		context.write(new Text("one rating for value of money"), new IntWritable(total_money));
    }
}