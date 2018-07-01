package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FacebookFriendsMapper extends Mapper<LongWritable, Text, FriendPair, FriendArray> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FriendPair, FriendArray>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//Logger log = Logger.getLogger(FacebookFriendsMapper.class);
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		String person = st.nextToken();
		String friends = st.nextToken();
		
		Friend f1=populateFriend(person);
		List<Friend> friendList=populateFriendList(friends);
		Friend[] fArray=Arrays.copyOf(friendList.toArray(), friendList.toArray().length, Friend[].class);
		FriendArray friendArray=new FriendArray(Friend.class, fArray);
		for(Friend f2:friendList){
			FriendPair fPair=new FriendPair(f1, f2);
			context.write(fPair, friendArray);
			//log.info(fPair+"......."+friendArray);
		}
	}

	private Friend populateFriend(String person){
		JSONParser parser=new JSONParser();
		Friend friend=null;
		try{
			Object obj=(Object)parser.parse(person);
			JSONObject jsonObject=(JSONObject)obj;
			
			Long lid=(Long)jsonObject.get("id");
			IntWritable id=new IntWritable(lid.intValue());
			
			Text name=new Text((String)jsonObject.get("name"));
			Text homeTown=new Text((String)jsonObject.get("hometown"));
			friend=new Friend(id, name, homeTown);
			
			
		}catch(ParseException e){
			e.printStackTrace();
		}
		return friend;
	}
	private List<Friend> populateFriendList(String friendsJSON){
		List<Friend> friendList=new ArrayList<Friend>();
		try{
			JSONParser parser=new JSONParser();
			Object obj=(Object)parser.parse(friendsJSON);
			JSONArray jsonArray=(JSONArray)obj;
			
			for(Object jobj:jsonArray){
				JSONObject jsonObject=(JSONObject)jobj;
				Long lid=(Long)jsonObject.get("id");
				IntWritable id=new IntWritable(lid.intValue());
				
				Text name=new Text((String)jsonObject.get("name"));
				Text homeTown=new Text((String)jsonObject.get("hometown"));
				Friend friend=new Friend(id, name, homeTown);
				friendList.add(friend);
				
			}
		}catch (ParseException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return friendList;
	}
}
