import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NasdaqMapperTop extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer nasRecord = new StringTokenizer(value.toString(), ",");
		
		if (nasRecord.countTokens() != 7) {
			System.out.println("Improper length of the record "
					+ nasRecord.countTokens());
			return;
		}
		
		String company = nasRecord.nextToken();
		if (company.equals("Symbol")) {
			System.out.println("Reading Header");
			return;
		}
		  
		
		int i =1;
		String opkey = company;
		String maxValue="", minValue="";
		String date = "";
		
		while (nasRecord.hasMoreTokens()) {
		//	System.out.println("Ticker Name " + opkey);
			
			if (i==1) {
			     date  = nasRecord.nextToken().toString();
			     System.out.println("date in mapper "+ date);
			}
			if (i==3) {
			     maxValue  = nasRecord.nextToken();
			     System.out.println("max value in mapper " + maxValue);
			}
			else if (i==4) {
				 minValue  = nasRecord.nextToken();
				 System.out.println("min value in mapper "+ minValue);
			}
			else
			   nasRecord.nextToken();
				i++;
		}
		System.out.println("key in Mapper " + opkey);
		
			context.write(new Text(opkey), new Text(date + ":" + minValue + ":" + maxValue));
		
	}

}
