package churncustomers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChurnReducer extends Reducer<Text, Text, Text, Text> 
{
    
    @Override
    public void reduce(Text word, Iterable<Text> value, Context con)throws IOException, InterruptedException
	{     
            HashMap<String, Integer> month = new HashMap<>(); 
            HashMap<String, Integer> reason = new HashMap<>(); 
            int cons = 0;
            String large="";
            for(Text x:value)
            {
                String[] fields = x.toString().trim().split(",");
                if(month.containsKey(fields[0]))
                {
                    int count = month.get(fields[0])+1;
                    month.put(fields[0], count);
                }
                else
                {
                    month.put(fields[0], 1);
                }
                
                if(Integer.parseInt(fields[1])<=3)
                {
                    if(reason.containsKey(fields[2]))
                    {
                        int count = reason.get(fields[2])+1;
                        reason.put(fields[2], count);
                    }
                else
                    {
                        reason.put(fields[2], 1);
                    }
                }
            }
            int orders = 0;
                
                for(Map.Entry<String,Integer> e: month.entrySet())
                {
                    if(orders!=0)
                    {
                        int latest = e.getValue();
                        if(latest<=(orders/2))
                        {
                            cons++;
                        }
                        else
                        {
                                cons = 0;
                        }
                    }
                    orders = e.getValue();
                }
                int largest = 0;
                
                for(Map.Entry<String,Integer> e: reason.entrySet())
                {
                   if(e.getValue()>largest)
                   {
                       largest=e.getValue();
                       large = e.getKey();
                   }
                }
              if(cons >=2)
                con.write(word, new Text(large));
            
        }
    
}
