import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NasdaqReducerTop extends Reducer<Text, Text, Text, Text> {
  Double gmin = 100000000000000.0;
  Double gmax = 0.0;
	
	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		 

		Double minVal = 1000000000000000000.0;
		Double maxVal = 0.0;
		Double top1 = 0.0;
		Double top2 =0.0;
		Double top3 = 0.0;
		String date1 = "";
		String date2 = "";
		String date3 = "";
		
		System.out.println("Reading key " + key.toString());
		System.err.println("Reading key " + key.toString());
		

		
		//for(Text minMaxString : values) {
			
			//Iterable
			
			
			while (values.iterator().hasNext()) {
			//tokenize on :
			
			StringTokenizer minMaxTokens = new StringTokenizer(values.iterator().next().toString(), ":");
			
			//part before :
			String date = minMaxTokens.nextToken();
			System.out.println("date value is -- " + date);
			Double minToken = Double.parseDouble(minMaxTokens.nextToken());
			System.out.println("minToken for " + key.toString() + "is " + minToken);
			System.err.println("minToken for " + key.toString() + "is " + minToken);
			
			//part after
			Double maxToken = Double.parseDouble(minMaxTokens.nextToken());
			System.out.println("maxToken for " + key.toString() + "is " + maxToken);
			System.err.println("maxToken for " + key.toString() + "is " + maxToken);
			if(minToken < minVal)
				minVal = minToken;
			
			if(maxToken > maxVal) {
				maxVal = maxToken;
				top1 = maxToken;
				date1 = date;
			}
				if (maxToken < top1 && maxToken > top2) {
					top2 = maxToken;
					date2 = date;
				}
				if (maxToken < top2 && maxToken > top3) {
					top3 = maxToken;
					date3 = date;
				}
				
		//	}
			
					
		}
			
//		
			System.out.println("key in reducer "+ key);
			System.err.println("key in reducer "+ key);
		context.write(key, new Text(minVal + ":" + maxVal + ": Date1 " + date1 + " -- " + top1 + " date2  " + date2.toString() +" -- " + top2.toString() + " : Date3 " + date3.toString() + " -- "  + top3));
		
	}
}
