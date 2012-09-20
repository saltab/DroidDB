package edu.buffalo.cse.cse486_586.simpledht;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import android.util.Log;
import edu.buffalo.cse.cse486_586.simpledht.provider.MessageWrapper;

/*
 * Purpose of this Class: To provide supporting methods to Client Thread
 * Methods here:
 * # sendToSequencer(): Send the data to sequencer
 */

public class ClientThread {
	
	private static 	Socket				sendToSeqSocket;	
	private	static 	ObjectOutputStream 	out;
	
	
	protected static void send(MessageWrapper message) throws IOException{
		try {
			sendToSeqSocket = new Socket("10.0.2.2", message.destPort);
			if(sendToSeqSocket.isConnected())
    			Log.d("ClienThread", "Connection Establised with Coordinator");
    			out = new ObjectOutputStream(sendToSeqSocket.getOutputStream());
    			//Send Data To Sequencer
    			out.writeObject(message);
    			Log.d("ClienThread", "Sent jREQ to Coordinator");
    			//sendToSeqSocket.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	/*protected static void B_Multicast(MessageWrapper message){
		try{
			for(int port : GlobalData.portArray){
				multicastSocket = new Socket("10.0.2.2",port);
				if(multicastSocket.isConnected())
	    			Log.d("DroidChatActivity", "Client-Server: Connection Establised with Emulator:"+(port/2));
				out2 = new ObjectOutputStream(multicastSocket.getOutputStream());
    			//Send Data To All peers
    			out2.writeObject(message);
    			Log.d("DroidChatActivity", "Client-Server: Sent Data to Emulator: "+(port/2));
	    		multicastSocket.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}*/	
	
}
