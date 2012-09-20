package edu.buffalo.cse.cse486_586.simpledynamo;

/*
 * This is the class of data that can be accessed by all, though, sequence No is accessible to Sequencer Thread
 * 
 */
public class GlobalData {

	public static final int serverPort = 10000; // Data sent to portArray ==>
												// 10000
	public static boolean flag = false;
	public static boolean isDBEmpty = true;
	public static boolean isDataReplicated = false;
	public static int MAX_NODE = 5;
	public static int seqNo;
	
	
	public static final int N = 3, W = 2, R = 2;
	}
