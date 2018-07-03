package com.example.pigpagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PageRankLoad extends LoadFunc{

	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
	private static final BagFactory bagFactory = BagFactory.getInstance();
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;
	
	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new XmlInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		try {
			Tuple tuple = tupleFactory.newTuple(2);//tuple with two columns
			DataBag bag = bagFactory.newDefaultBag();
			
			if(!reader.nextKeyValue()) {
				return null;
			}
			//Wikipedia Page
			Text page = (Text)reader.getCurrentValue();
			
			String pageTitle = new String();
			String pageContent = new String();
			
			int begin = page.find("<title>");
			int end = page.find("</title>",begin);
			
			pageTitle = Text.decode(page.getBytes(), begin+7, end-(begin+7));
			
			if(pageTitle.contains(":")) {
				return tuple;
			}
			
			begin = page.find(">",page.find("<text"));
			end = page.find("</text>",begin);
			
			if(begin+1 == -1  || end== -1) {
				pageTitle = "";
				pageContent = "";
			}
			pageContent = Text.decode(page.getBytes(), begin+1, end - (begin+1));
			
			tuple.set(0, new DataByteArray(pageTitle.replace(' ', '_').toString()));
			
			Pattern findWikiLinks = Pattern.compile("\\[.+?\\]");
			Matcher matcher = findWikiLinks.matcher(pageContent);
			
			while(matcher.find()) {
				String linkPage = matcher.group();
				
				linkPage = getPage(linkPage);
				
				if(linkPage == null || linkPage.isEmpty()) {
					continue;
				}
				
				Tuple ltuple = tupleFactory.newTuple(1);
				ltuple.set(0, linkPage);
				bag.add(ltuple);
			}
			
			tuple.set(1, bag);
			return tuple;
		}catch (InterruptedException e) {
			// TODO: handle exception
			throw new ExecException(e);
		}
	}
	 

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		// TODO Auto-generated method stub
		this.reader = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(job, location);
	}
	
	private String getPage(String linkPage) {
		boolean isGoodLink = true;
		int srt = 1;
		if(linkPage.startsWith("[[")) {
			srt=2;
		}
		
		int end = linkPage.indexOf("#") > 0 ? linkPage.indexOf("#") : linkPage.indexOf("|") >0 ?
					linkPage.indexOf("|") : linkPage.indexOf("]")>0?
							linkPage.indexOf("]"):linkPage.indexOf("|");

		if (linkPage.length() <srt+2 || linkPage.length() >100 || linkPage.contains(":")||
				linkPage.contains(",")||linkPage.contains("&")) {
			isGoodLink=false;
		}
		
		char firstChar = linkPage.charAt(srt);
		
		if(firstChar == '#' || firstChar == ',' || firstChar == '\''||
				firstChar == '-' || firstChar == '(') {
			isGoodLink = false;
		}
		if(!isGoodLink) {
			return null;
		}
		
		linkPage = linkPage.substring(srt, end);
		linkPage = linkPage.replaceAll("\\s", "_").replaceAll(",", "").replaceAll("&amp", "&");
		
		return linkPage;
	}
}
