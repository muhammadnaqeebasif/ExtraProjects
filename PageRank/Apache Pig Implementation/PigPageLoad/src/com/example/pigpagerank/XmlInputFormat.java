package com.example.pigpagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XmlInputFormat extends TextInputFormat {
	
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		// TODO Auto-generated method stub
		
		try {
			return new XmlRecodeReader((FileSplit)split, context);
		}catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException("TODO : refractor this....");
		}
	}
	
	public static class XmlRecodeReader extends RecordReader<LongWritable, Text>{
		
		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private final FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
		
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		
		public XmlRecodeReader(FileSplit split,TaskAttemptContext context) throws IOException {
			// TODO Auto-generated constructor stub
			Configuration conf = context.getConfiguration();
			startTag = new String("<page>").getBytes("utf-8");
			endTag = new String("</page>").getBytes("utf-8");
			
			//open the file and seek to the start of the split
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);
			fsin = fs.open(split.getPath());
			
			start = split.getStart();
			end = start + split.getLength();
			
			fsin.seek(start);
		}
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return (fsin.getPos() - start) /(float)(end -start);
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(fsin.getPos()<end) {
				if(readUntillMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if(readUntillMatch(endTag, true)) {
							key.set(fsin.getPos());
							value.set(buffer.getData(),0,buffer.getLength());
							return true;
						}
					}finally {
						buffer.reset();
					}
				}
			}
			return false;
		}
		
		private boolean readUntillMatch(byte[] match,boolean withinBlock) throws IOException {
			int i=0;
			while(true) {
				int b = fsin.read();
				if(b==-1) {
					return false;
				}
				if(withinBlock) {
					buffer.write(b);
				}
				
				if(b==match[i]) {
					i++;
					if(i>=match.length)
						return true;
				}
				else {
					i = 0;
					if(!withinBlock && i==0 && fsin.getPos()>=end) {
						return false;
					}
				}
			}
		}
		
	}
}
