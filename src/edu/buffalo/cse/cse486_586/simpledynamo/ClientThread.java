package edu.buffalo.cse.cse486_586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import android.util.Log;
import edu.buffalo.cse.cse486_586.simpledynamo.provider.MessageWrapper;

public class ClientThread {

	private static Socket sendSock;
	private static ObjectOutputStream out;
	private static ObjectInputStream in;

	protected static MessageWrapper send(MessageWrapper message)
			{

		MessageWrapper retMsg = new MessageWrapper();
		try {
			sendSock = new Socket("10.0.2.2", message.destPort);
			if (sendSock.isConnected()) {
				Log.d("ClientThread", "Connection Establised");

				out = new ObjectOutputStream(sendSock.getOutputStream());
				out.writeObject(message);

				int timeout = 5000;
				sendSock.setSoTimeout(timeout);
				Log.d("ClientThread", "Socket Timeout Set to: " + timeout);
				
				in = new ObjectInputStream(sendSock.getInputStream());
				retMsg = (MessageWrapper) in.readObject();
				Log.d("ClientThread", "Received reply");
				sendSock.close();
			}
			return retMsg;
		} catch(IOException e){
			e.printStackTrace();
			Log.d("ClientThread", "Dest Node is DOWN");
			
			retMsg.messageType = "null";			
			return retMsg;
		}catch (ClassNotFoundException c) {
			c.printStackTrace();
			Log.d("ClientThread", "UNKNOWN Situation");
			return null;
		} /*catch (InterruptedIOException io) {
			io.printStackTrace();
			
		}*/

	}
}
