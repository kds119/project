package com.bits.hadoop;

import java.util.HashMap;

public class Mappings {

	//private String key1 = "IP";
	//private String key1 = "IP";
	//private String key1 = "IP";
	//private String key1 = "I";
	
	/*private String one = "No";
	private String two = "Time";
	private String one = "Source";
	private String one = "Destination";
	private String one = "Protocol";*/
	
	String[] str;

	HashMap<String, Integer> hm;
	/*
	public void setup()
	{
		// 1 represents the index here or place in the input file 
		hm.put(key1 , 1);
	}
	*/
	public Integer getIndex(String key)
	{
		setup();
		return hm.get(key);	
	}
	
	public void setup()
	{
		hm = new HashMap<String, Integer>();
		str = new String[]{"No","Time","Source","Destination","Protocol","Length","Info"};
		
		//int count =0;
		
		for(int i=0;i<str.length;i++)
		{
			hm.put(str[i], i);
		}
	}
}
