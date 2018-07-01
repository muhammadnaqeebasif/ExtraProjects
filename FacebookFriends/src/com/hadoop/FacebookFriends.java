package com.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FacebookFriends {

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: FacebookFriends <input> <output>");
			System.exit(1);
		}
		//FileUtils.deleteDirectory(new File(args[1]));
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(FacebookFriends.class);
		job.setJobName("FacebookFriends");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(FacebookFriendsMapper.class);
		job.setReducerClass(FacebookFriendsReducer.class);

		job.setMapOutputKeyClass(FriendPair.class);
		job.setMapOutputValueClass(FriendArray.class);

		job.setOutputKeyClass(FriendPair.class);
		job.setOutputValueClass(FriendArray.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
