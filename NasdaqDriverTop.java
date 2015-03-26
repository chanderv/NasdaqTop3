import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NasdaqDriverTop {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
//		Configuration conf = new Configuration(true);
		
//		Job job = new Job(conf, "Nasdaq Driver");
		Job job = new Job();
		
		job.setJarByClass(NasdaqDriverTop.class);
		job.setJobName("nasdaq driver");
		
		job.setMapperClass(NasdaqMapperTop.class);
		job.setReducerClass(NasdaqReducerTop.class);
		job.setCombinerClass(NasdaqReducerTop.class);
		
	//	job.setInputFormatClass(TextInputFormat.class);
	  //  job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
				

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
