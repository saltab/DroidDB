/*
package edu.buffalo.cse.cse486_586.simpledynamo;

import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Handler;
import android.telephony.TelephonyManager;
import android.util.Log;

public class CopyOfprovider extends ContentProvider {
	 variables required for ring setup and communication1 
	Handler handler = new Handler();
	MessageWrapper recvMsg;
	
	List<String> ring = new LinkedList<String>();
	List<String> tmpRing = new LinkedList<String>();
	List<Integer> portArray;
	HashMap<String, Integer> mapPort;
	
	String pred, succ, portStr;
	int predPort, succPort;
	String myID;
	Uri myUri = Uri.parse("content://" + CopyOfprovider.AUTHORITY + "/MessageT");

	 Matrix Cursor 
	String[] menu = { "provider_key", "provider_value" };
	MatrixCursor mCursor = new MatrixCursor(menu);

	// info required for building SQLite based Content Provider
	private static UriMatcher UriCheck;
	private static final String dbName = "MessageDB";
	private static final int dbVersion = 21;
	private static final String tableName = "MessageT";
	private static final int messages = 1;

	// Content Provider URI
	public static final String AUTHORITY = "edu.buffalo.cse.cse486_586.simpledht.provider";
	public static HashMap<String, String> msgProjMap;

	// SQL query to create the SQLite Database
	private static final String sqlCreateDB = "CREATE TABLE " + tableName + "("
			+ Message.MSG_ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
			+ Message.provider_key + " LONGTEXT," + Message.provider_value
			+ " LONGTEXT );";

	private static class MyDB extends SQLiteOpenHelper {

		public MyDB(Context context) {
			super(context, dbName, null, dbVersion);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			// TODO Auto-generated method stub
			db.execSQL(sqlCreateDB);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// TODO Auto-generated method stub
			db.execSQL("DROP TABLE IF EXISTS " + tableName);
			onCreate(db);

		}

	}

	MyDB createDB;

	@Override
	public int delete(Uri uri, String arg1, String[] arg2) {
		// TODO Auto-generated method stub
		SQLiteDatabase db = createDB.getWritableDatabase();
		int count;
		switch (UriCheck.match(uri)) {
		case messages:
			count = db.delete(tableName, arg1, arg2);
			break;
		default:
			throw new IllegalArgumentException("Unknown URI " + uri);
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return count;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key, keyHash = null;

		if (UriCheck.match(uri) != messages)
			throw new IllegalArgumentException("Arguments are wrong");
		ContentValues insertV;
		if (values != null)
			insertV = new ContentValues(values);
		else
			insertV = new ContentValues();

		key = insertV.getAsString(Message.provider_key);
		Log.d("INSERT from provider", "Key read is :" + key);
		try {
			keyHash = genHash(key);
			Log.d("Provider: insert", "keyHash generated:" + keyHash);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// tmpRing =ring;
		String retHash = lookup(keyHash);

		if (!retHash.contentEquals(myID)) {
			Log.d("Provider: insert", "Remote Insert operation");
			MessageWrapper remoteMsg = new MessageWrapper();
			remoteMsg.destPort = mapPort.get(succ);
			remoteMsg.devID = portStr;
			remoteMsg.devHash = myID;
			remoteMsg.key = key;
			remoteMsg.originPort = mapPort.get(myID);
			remoteMsg.messageType = "iREQ";

			ClientThread(remoteMsg, 1);
			return null;
		} else {
			Log.d("Provider: insert", "Local Insert operation");
			 Local Insert Method 
			SQLiteDatabase writeDB = createDB.getWritableDatabase();
			long rID = writeDB.insert(tableName, null, insertV);
			if (rID > 0) {
				Uri _uri = ContentUris.withAppendedId(uri, rID);
				getContext().getContentResolver().notifyChange(
						Message.CONTENT_URI, null);
				return _uri;
			}
			throw new SQLException("Failed to insert into " + uri);
		}

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		
		// delete the previous database
		getContext().deleteDatabase(dbName);
		
		// create a new database
		createDB = new MyDB(getContext());
		
		// get the emulator port for this Content Provider
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		
		mapPort = new HashMap<String, Integer>();
		portArray = new ArrayList<Integer>();
				
		pred = null;
		succ = null;

		try {
			 Initialize all the variables 

			myID = genHash(portStr);
			ring.add(myID);
			
			mapPort.put(myID, Integer.parseInt(portStr) * 2);

			 Send a node-join request to 5554 
			if (!portStr.contentEquals("5554")) {
				MessageWrapper req = new MessageWrapper();
				req.destPort = 11108;
				req.devID = portStr;
				req.devHash = myID;
				req.messageType = "jREQ";
				ClientThread(req, -1);
			}

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		 Start the server thread 
		ServerThread();

		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String keyToRead,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		 Lets check if the key is local or remote 
		if (keyToRead == null) {
			Log.d("Query", "Dump QUERY operation");
			SQLiteQueryBuilder queryDB = new SQLiteQueryBuilder();
			switch (UriCheck.match(uri)) {
			case messages:
				queryDB.setTables(tableName);
				queryDB.setProjectionMap(msgProjMap);
				break;

			default:
				throw new IllegalArgumentException("Unknown URI " + uri);

			}
			SQLiteDatabase readDB = createDB.getReadableDatabase();
			Cursor cursor = queryDB.query(readDB, projection, null, null, null,
					null, sortOrder);
			cursor.setNotificationUri(getContext().getContentResolver(), uri);
			return cursor;

			} else {

			String key, keyHash = null;
			String retHash = null;
			key = keyToRead;

			try {
				keyHash = genHash(key);
				Log.d("Provider: insert", "keyHash generated:" + keyHash);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			retHash = lookup(keyHash);
			if(!retHash.contentEquals(myID)){
			Log.d("Provider: query", "Remote Query operation");

			if (GlobalData.flag) {
				MessageWrapper remoteMsg = new MessageWrapper();
				remoteMsg.destPort = mapPort.get(succ);
				remoteMsg.devID = portStr;
				remoteMsg.devHash = myID;
				remoteMsg.key = key;
				remoteMsg.originPort = mapPort.get(myID);
				remoteMsg.messageType = "qREQ";

				ClientThread(remoteMsg, 2);

				try {
					synchronized (mCursor) {
						mCursor.wait();
					}
					Log.d("Content Provider", "mCursor is Notified");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return mCursor;
			} else
				return null;
			}
			else{
				Log.d("Query", "Local QUERY operation");
				SQLiteQueryBuilder queryDB = new SQLiteQueryBuilder();
				switch (UriCheck.match(uri)) {
				case messages:
					queryDB.setTables(tableName);
					queryDB.setProjectionMap(msgProjMap);
					break;

				default:
					throw new IllegalArgumentException("Unknown URI " + uri);

				}
				SQLiteDatabase readDB = createDB.getReadableDatabase();
				Cursor cursor = queryDB.query(readDB, projection, null, null, null,
						null, sortOrder);
				cursor.setNotificationUri(getContext().getContentResolver(), uri);
				return cursor;

			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	static {
		UriCheck = new UriMatcher(UriMatcher.NO_MATCH);
		UriCheck.addURI(AUTHORITY, tableName, messages);
		UriCheck.addURI(AUTHORITY, tableName + "/local", messages);

		msgProjMap = new HashMap<String, String>();
		msgProjMap.put(Message.MSG_ID, Message.MSG_ID);
		msgProjMap.put(Message.provider_key, Message.provider_key);
		msgProjMap.put(Message.provider_value, Message.provider_value);
	}

	public static class MessageWrapper implements Serializable {
		*//**
		 * MESSAGE FORMAT USED FOR MY SimpleDHT
		 *//*
		private static final long serialVersionUID = 1L;
		int destPort;
		int originPort;
		String key;
		String messageType; // messageType = iREQ /jREQ/ iACK / jACK /
		String devHash;
		String devID; // devID = 5554 / 5556 / 5558 / 5560 / 5562
		List<String> ring;
		HashMap<String, Integer> map;
	}

	 Generate has value for each provider 
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	 Definition of ClientThread 
	protected synchronized void ClientThread(final MessageWrapper req, int _case) {
		Runnable runnable = new Runnable() {
			public void run() {
				try {
					if (req.messageType.contentEquals("jREQ")) {
						ClientThread.send(req);
						Log.d("ClientThread",
								"New Node: jREQ sent successfully");
					}

					if (req.messageType.contentEquals("jACK")) {
						ClientThread.send(req);
						Log.d("ClientThread",
								"Coordinator: jACK sent successfully");
					}

					if (req.messageType.contentEquals("iREQ")) {
						ClientThread.send(req);
						Log.d("ClientThread",
								"Remote Request to Insert successful");
					}

					if (req.messageType.contentEquals("qREQ")) {
						ClientThread.send(req);
						Log.d("ClientThread",
								"Remote Request to Query successful");
					}
					if (req.messageType.contentEquals("qACK")) {
						ClientThread.send(req);

					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		// Start The ClientThread
		new Thread(runnable).start();
		
		 * try { Thread.sleep(3000); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 
	}

	 ServerThread Definition 
	protected void ServerThread() {
		Runnable runnable = new Runnable() {
			public void run() {
				ServerThread.startServer();

				while (true) {
					try {
						recvMsg = ServerThread.pollData();
					} catch (IOException e) {
						e.printStackTrace();
					}
					 Handle the jREQ from node 
					if (recvMsg.messageType.contentEquals("jREQ")) {
						portArray.add(Integer.parseInt(recvMsg.devID));
						mapPort.put(recvMsg.devHash,
								Integer.parseInt(recvMsg.devID) * 2);
						
						Log.d("ServerThread", "Node Join jREQ from node: "
								+ recvMsg.devID);
						if (!ring.contains(recvMsg.devHash))
							ring.add(recvMsg.devHash);
						reformRing(ring, recvMsg.devHash);

						for (int port : portArray) {
							MessageWrapper reply = new MessageWrapper();
							reply.destPort = port * 2;
							reply.devID = portStr;
							reply.devHash = myID;
							reply.messageType = "jACK";
							reply.ring = ring;
							reply.map = mapPort;
							 Acknowledge the new node 

							ClientThread(reply, -1);
						}
					}

					if (recvMsg.messageType.contentEquals("jACK")) {
						Log.d("ServerThread",
								"Node join Ack received from Coordinator");
						ring = recvMsg.ring;
						mapPort = recvMsg.map;
						reformRing(ring, recvMsg.devHash);

					}

					if (recvMsg.messageType.contentEquals("iREQ")) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						Log.d("ServerThread",
								"Insert CV REQ received from Emulator "
										+ (recvMsg.originPort / 2));
						 Update the new key 
						GlobalData.seqNo = Integer.parseInt(recvMsg.key);

						ContentValues valuesCV = new ContentValues();
						valuesCV.put(Message.provider_key, recvMsg.key);
						valuesCV.put(Message.provider_value, "TEST"
								+ recvMsg.key);
						insert(myUri, valuesCV);
					}

					if (recvMsg.messageType.contentEquals("qREQ")) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						Log.d("ServerThread", "qREQ received");
						GlobalData.seqNo = Integer.parseInt(recvMsg.key);
						Cursor cur = query(myUri, null, recvMsg.key, null,
								Message.provider_key + " ASC");
						if (cur == null) {
							Log.d("ServerThread", "qREQ is fwd");
							MessageWrapper fwdMsg = new MessageWrapper();
							fwdMsg.destPort = mapPort.get(succ);
							fwdMsg.devID = portStr;
							fwdMsg.devHash = myID;
							fwdMsg.key = recvMsg.key;
							fwdMsg.messageType = "qREQ";
							fwdMsg.originPort = recvMsg.originPort;
							ClientThread(fwdMsg, 2);
						} else {
							// if(cur.moveToFirst())
							String retKey = null;
							boolean flag = false;
							cur.moveToFirst();
							;
							while (!cur.isLast()) {
								retKey = cur.getString(cur
										.getColumnIndex(Message.provider_key));
								if (retKey.contentEquals(recvMsg.key)) {
									flag = true;
									break;
								}
								cur.moveToNext();
							}
							if (cur.getString(
									cur.getColumnIndex(Message.provider_key))
									.contentEquals(recvMsg.key)
									&& flag == false)
								retKey = cur.getString(cur
										.getColumnIndex(Message.provider_key));
							Log.d("ServerThread", "qREQ is local & retKey is: "
									+ retKey);
							MessageWrapper remoteMsg = new MessageWrapper();
							remoteMsg.destPort = mapPort.get(pred);
							remoteMsg.devID = portStr;
							remoteMsg.devHash = myID;
							remoteMsg.key = retKey;
							remoteMsg.messageType = "qACK";
							remoteMsg.originPort = recvMsg.originPort;
							ClientThread(remoteMsg, 2);

						}
					}

					if (recvMsg.messageType.contentEquals("qACK")) {

						Log.d("ServerThread", "qACK received");
						Log.d("ServerThread", "Origin Port:"
								+ recvMsg.originPort + " & my Port is: "
								+ mapPort.get(myID));
						if (recvMsg.originPort == mapPort.get(myID)) {
							Log.d("ServerThread",
									" Finally Got the reply of qREQ");
							// mCursor = new MatrixCursor(menu);
							// mCursor.addRow(new
							// Object[]{recvMsg.key,"TEST"+recvMsg.key});
							mCursor.newRow().add(recvMsg.key)
									.add("TEST" + recvMsg.key);

							Log.d("ServerThread", "mCursor Created");
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							synchronized (mCursor) {
								mCursor.notifyAll();
							}

						} else {
							MessageWrapper fwdMsg = new MessageWrapper();
							fwdMsg.destPort = mapPort.get(pred);
							fwdMsg.devID = portStr;
							fwdMsg.devHash = myID;
							fwdMsg.key = recvMsg.key;
							fwdMsg.messageType = recvMsg.messageType;
							fwdMsg.originPort = recvMsg.originPort;
							ClientThread(fwdMsg, 2);
							Log.d("ServerThread",
									"qACK forwarded to Origin Port");
						}
					}

				}
			}
		};
		// Start The ClientThread
		new Thread(runnable).start();
		
		 * try { Thread.sleep(3000); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 
	}

	protected void reformRing(List<String> ring, String devHash) {

		Collections.sort(ring);
		int size = ring.size();
		int index = ring.indexOf(myID);

		if (size == 2) {
			pred = devHash;
			succ = devHash;
		} else if ((index - 1) < 0) {
			pred = ring.get(size - 1);
			succ = ring.get(index + 1);
		} else if ((index + 1) > size - 1) {
			succ = ring.get(0);
			pred = ring.get(index - 1);
		} else {
			pred = ring.get(index - 1);
			succ = ring.get(index + 1);
		}

		System.out.println("Current entries in ring: " + ring);
		System.out.println("My " + portStr + " pred is: " + pred
				+ "& succ is: " + succ);
	}

	 Method called to check if the key is local or remote 
	protected String lookup(String keyHash) {

		ring.add(keyHash);
		Collections.sort(ring);

		int size = ring.size();
		int index = ring.indexOf(keyHash);

		if ((index - 1) < 0 || (index + 1) > size - 1) {
			ring.remove(ring.indexOf(keyHash));
			Collections.sort(ring);
			return ring.get(0);
		} else {
			ring.remove(ring.indexOf(keyHash));
			Collections.sort(ring);
			return ring.get(index);
		}
	}
}
*/