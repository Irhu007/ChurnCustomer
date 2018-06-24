/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package churncustomers;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChurnMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        int month =0;
        String[] fields = value.toString().trim().split(",");
        
        String[] date = fields[1].trim().split("-");
        con.write(new Text(fields[0]), new Text(date[1]+","+fields[8]+","+fields[9]));
    }
    
}
