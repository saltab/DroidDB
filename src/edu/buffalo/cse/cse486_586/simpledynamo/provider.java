package edu.buffalo.cse.cse486_586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
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
import android.telephony.TelephonyManager;
import android.util.Log;

public class provider extends ContentProvider {

	public static class MessageWrapper implements Serializable {
		/*
		 * MESSAGE FORMAT USED FOR MY SimpleDynamo
		 */
		private static final long serialVersionUID = 1L;
		int destPort;
		int originPort;
		String key, value;

		// used at Network Boot time
		List<String> keySet;
		List<String> valueSet;

		String messageType; // messageType = learnREQ / learnACK / learnNACK
		// putREQ / putACK / getREQ / getACK / replicateREQ / replicateACK /
		// voteREQ / voteACK
		int reqType; // reqType = 0 -> to coordinator / 1 -> succ1 / 2->
						// succ2
	}

	/* Messages which will be required further */

	MessageWrapper bootMsg;
	MessageWrapper bootReply;
	MessageWrapper putMsg;
	MessageWrapper putReply;
	MessageWrapper getMsg;
	MessageWrapper getReply;
	MessageWrapper voteMsg;
	MessageWrapper voteReply;
	MessageWrapper replicateMsg;
	MessageWrapper replicateReply;

	ServerSocket listenSocket;
	Socket sckt;
	ObjectInputStream in;
	ObjectOutputStream out;
	BufferedReader bufReader;
	MessageWrapper recvMsg;
	MessageWrapper replyMsg;

	// data structure to store the static members in the ring
	List<String> ring = new LinkedList<String>();

	// static store of all the port numbers
	List<Integer> portArray = new ArrayList<Integer>();;

	// static reverse store of all hash values to port number
	HashMap<String, Integer> mapPort = new HashMap<String, Integer>();

	boolean[] isKeyValid = new boolean[10];

	String succ1, succ2, myID, myHash, tempHash;

	Uri myUri = Uri.parse("content://" + provider.AUTHORITY + "/MessageT");

	// matrix cursor used for returning received data to UI thread
	String[] menu = { "provider_key", "provider_value" };
	MatrixCursor mCursor = new MatrixCursor(menu);
	MatrixCursor matCur = new MatrixCursor(menu);

	// info required for building SQLite based Content Provider
	private static UriMatcher UriCheck;
	private static final String dbName = "MessageDB";
	private static final int dbVersion = 21;
	private static final String tableName = "MessageT";
	private static final int messages = 1;

	// Content Provider URI
	public static final String AUTHORITY = "edu.buffalo.cse.cse486_586.simpledynamo.provider";
	public static final String AUTHORITY1 = "edu.buffalo.cse.cse486_586.simpledynamo.provider1";
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
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		SQLiteDatabase db = createDB.getWritableDatabase();
		// SQLiteDatabase rep = repDB.getWritableDatabase();

		int count;
		switch (UriCheck.match(uri)) {
		case messages:
			count = db.delete(tableName, selection, selectionArgs);
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
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key, keyHash = null, value;
		ContentValues insertV;

		if (values != null)
			insertV = new ContentValues(values);
		else
			insertV = new ContentValues();

		key = insertV.getAsString(Message.provider_key);
		value = insertV.getAsString(Message.provider_value);
		Log.d("INSERT from provider", "Key/Value read is :" + key + " " + value);
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (!isKeyValid[Integer.parseInt(key)]) {
			if (uri.compareTo(myUri) == 0) {
				String retHash = lookup(keyHash);
				// check if this key belong to me
				if (!retHash.contentEquals(myHash)) {
					Log.d("Provider: insert", "Remote Put operation");
					putMsg = new MessageWrapper();
					putMsg.destPort = mapPort.get(retHash);
					putMsg.key = key;
					putMsg.value = value;
					putMsg.originPort = mapPort.get(myHash);
					putMsg.messageType = "putREQ";

					ClientThread(putMsg);
					return null;
				} else {
					Log.d("Provider: insert", "Local Put operation at " + myID);

					SQLiteDatabase writeDB = createDB.getWritableDatabase();
					long rID = writeDB.insert(tableName, null, insertV);
					if (rID > 0) {
						Uri _uri = ContentUris.withAppendedId(myUri, rID);
						getContext().getContentResolver().notifyChange(
								Message.CONTENT_URI, null);
						GlobalData.isDBEmpty = false;
						Log.d("ClientThread",
								"Local Put Operation Successfull for key "
										+ key);
						isKeyValid[Integer.parseInt(key)] = true;

						if (!GlobalData.isDataReplicated) {
							// lets replicate this insertion at succ1 and succ2
							MessageWrapper tempMsg = new MessageWrapper();
							tempMsg.key = key;
							tempMsg.value = value;
							tempMsg.messageType = "replicateREQ";
							ClientThread(tempMsg);
						}
						return _uri;
					}
					throw new SQLException("Failed to insert into " + uri);
				}
			} else {

				SQLiteDatabase writeDB = createDB.getWritableDatabase();
				long rID = writeDB.insert(tableName, null, insertV);
				if (rID > 0) {
					Uri _uri = ContentUris.withAppendedId(myUri, rID);
					getContext().getContentResolver().notifyChange(
							Message.CONTENT_URI, null);
					GlobalData.isDBEmpty = false;
					return _uri;
				}
				throw new SQLException("Failed to insert into " + uri);
			}
		} else {
			Log.d("Insert", "Updating previous value of Key " + key);
			update(myUri, insertV, key, null);

			MessageWrapper tempMsg = new MessageWrapper();
			tempMsg.key = key;
			tempMsg.value = value;
			tempMsg.messageType = "updateREQ";
			ClientThread(tempMsg);

			return null;
		}

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		// delete the previous database
		getContext().deleteDatabase(dbName);

		// create a new database
		createDB = new MyDB(getContext());
		// initialize all the data structures
		intializeAll();
		// Start the server thread
		ServerThread();
		// Initialize the n/w boot phase
		bootMsg = new MessageWrapper();
		bootMsg.destPort = mapPort.get(succ1);
		bootMsg.originPort = mapPort.get(myHash);
		bootMsg.reqType = 1; // ask the succ1
		bootMsg.messageType = "bootREQ";
		bootMsg.keySet = new ArrayList<String>();
		// ask succ1
		ClientThread(bootMsg);

		return true;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection,
			String keyToRead, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		if (keyToRead == null || (uri.compareTo(myUri) != 0)) {
			// Dump operation or Replicated Data Request
			Log.d("Query",
					"Dump QUERY operation | Replicated Data REQ for key: "
							+ keyToRead);

			SQLiteQueryBuilder queryDB = new SQLiteQueryBuilder();
			queryDB.setTables(tableName);
			queryDB.setProjectionMap(msgProjMap);
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
			if (!retHash.contentEquals(myHash)) {
				Log.d("Provider: query", "Remote Query operation for key: "
						+ key);

				MessageWrapper remoteMsg = new MessageWrapper();
				remoteMsg.destPort = mapPort.get(retHash);
				remoteMsg.key = key;
				remoteMsg.originPort = mapPort.get(myHash);
				remoteMsg.messageType = "getREQ";

				getReply = new MessageWrapper();
				ClientThread(remoteMsg);

				synchronized (matCur) {
					try {
						matCur.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				return matCur;
			} else {
				Log.d("Query", "Local QUERY operation for key: " + key);
				SQLiteQueryBuilder queryDB = new SQLiteQueryBuilder();
				queryDB.setTables(tableName);
				queryDB.setProjectionMap(msgProjMap);

				SQLiteDatabase readDB = createDB.getReadableDatabase();
				Cursor cursor = queryDB.query(readDB, projection, null, null,
						null, null, sortOrder);
				cursor.setNotificationUri(getContext().getContentResolver(),
						uri);
				return cursor;

			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		SQLiteDatabase db = createDB.getWritableDatabase();
		int count;
		// update the database value for that content value
		count = db.update(tableName, values,
				Message.MSG_ID + " = " + selection, null);
		getContext().getContentResolver().notifyChange(uri, null);
		return count;
	}

	protected void ServerThread() {
		Runnable runnable = new Runnable() {

			@Override
			public void run() {

				// TODO Auto-generated method stub
				try {
					listenSocket = new ServerSocket(GlobalData.serverPort);
					Log.d("ServerThread", "Server: Server listening on port "
							+ GlobalData.serverPort);

					while (true) {
						recvMsg = new MessageWrapper();
						replyMsg = new MessageWrapper();
						sckt = listenSocket.accept();

						in = new ObjectInputStream(sckt.getInputStream());
						out = new ObjectOutputStream(sckt.getOutputStream());

						recvMsg = (MessageWrapper) in.readObject();
						Log.d("Server", "Received message: "
								+ recvMsg.messageType);

						String msgType = recvMsg.messageType;

						if (msgType.contentEquals("bootREQ")) {
							if (GlobalData.isDBEmpty) {
								Log.d("Server",
										"My DB is EMPTY ; Returning NACK");

								replyMsg.destPort = recvMsg.originPort;
								replyMsg.messageType = "bootNACK";
								replyMsg.originPort = mapPort.get(myHash);
								out.writeObject(replyMsg);
								Log.d("Server", "bootNACK sent");
							} else {
								Log.d("Server",
										"I got some data for you bootREQ");

								Cursor cur;
								String _key, _value;
								cur = query(myUri, null, null, null,
										Message.provider_key + " ASC");
								cur.moveToFirst();
								replyMsg.valueSet = new ArrayList<String>();
								while (!cur.isLast()) {
									_key = cur
											.getString(cur
													.getColumnIndex(Message.provider_key));
									_value = cur
											.getString(cur
													.getColumnIndex(Message.provider_value));
									Log.d("Server: bootREQ",
											"Received Key/Value as: " + _key
													+ " " + _value);
									for (String key : recvMsg.keySet) {
										if (_key.contentEquals(key)) {
											replyMsg.valueSet.add(_value);
											break;
										}
									}
									cur.moveToNext();
								}

								_key = cur.getString(cur
										.getColumnIndex(Message.provider_key));
								_value = cur
										.getString(cur
												.getColumnIndex(Message.provider_value));
								Log.d("Server: bootREQ",
										"Received Key/Value as: " + _key + " "
												+ _value);
								for (String key : recvMsg.keySet)
									if (_key.contentEquals(key))
										replyMsg.valueSet.add(_value);

								replyMsg.keySet = new ArrayList<String>();
								replyMsg.keySet = recvMsg.keySet;
								replyMsg.destPort = recvMsg.originPort;
								replyMsg.messageType = "bootACK";
								replyMsg.originPort = mapPort.get(myHash);

								out.writeObject(replyMsg);
								Log.d("Server", "bootACK sent by "
										+ replyMsg.originPort / 2 + " to "
										+ replyMsg.destPort / 2);
							}
						} else if (msgType.contentEquals("putREQ")) {
							ContentValues cv = new ContentValues();
							cv.put(Message.provider_key, recvMsg.key);
							cv.put(Message.provider_value, recvMsg.value);

							// lets take the voting for this request
							String retType1, retType2;

							Log.d("Client", "Starting Voting Phase for key: "
									+ recvMsg.key);

							retType1 = voteForMe(succ1, 1);
							Thread.sleep(1000);
							retType2 = voteForMe(succ2, 2);

							if (retType1.contentEquals("voteACK")
									|| retType2.contentEquals("voteACK")) {
								Log.d("ClientThread", "Voting Successful");

								// lets continue
								insert(myUri, cv);
								// Risk is here!!
								Log.d("ServerThread", "key:" + recvMsg.key
										+ " inserted successfully sent from: "
										+ recvMsg.originPort);
								// lets replicate this data
								/*
								 * Log.d("Server", "Replication Started at " +
								 * myID); replicateData(recvMsg, succ1, 1);
								 * replicateData(recvMsg, succ2, 2);
								 * Log.d("Server", "Replication Done");
								 */
								replyMsg.destPort = recvMsg.originPort;
								replyMsg.originPort = mapPort.get(myHash);
								replyMsg.messageType = "putACK";
								replyMsg.originPort = mapPort.get(myHash);

								out.writeObject(replyMsg);
								Log.d("ServerThread",
										"" + Integer.parseInt(myID)
												+ " sent putACK to "
												+ replyMsg.destPort / 2);
							} else {
								Log.d("Server: put",
										"Voting Unsuccessful! How did I reach here!!");
							}
						} else if (msgType.contentEquals("putRepREQ")) {

							Log.d("Server: PutRepREQ",
									"REQ to put data received from :"
											+ recvMsg.originPort);
							ContentValues cv = new ContentValues();
							cv.put(Message.provider_key, recvMsg.key);
							cv.put(Message.provider_value, recvMsg.value);
							Uri _uri = Uri.parse("content://"
									+ provider.AUTHORITY1);
							insert(_uri, cv);
							// Risk is here!!
							Log.d("Server: PutRepREQ", "key:" + recvMsg.key
									+ " inserted successfully sent from: "
									+ recvMsg.originPort);

						} else if (msgType.contentEquals("voteREQ")) {
							replyMsg.destPort = recvMsg.originPort;
							replyMsg.originPort = mapPort.get(myHash);
							replyMsg.messageType = "voteACK";
							replyMsg.originPort = mapPort.get(myHash);

							out.writeObject(replyMsg);
							Log.d("ServerThread", "" + Integer.parseInt(myID)
									+ " sent voteACK to " + replyMsg.destPort
									/ 2);

						} else if (msgType.contentEquals("replicateREQ")) {
							Log.d("Server", "Received replicateREQ from "
									+ recvMsg.originPort / 2);
							ContentValues cv = new ContentValues();
							cv.put(Message.provider_key, recvMsg.key);
							cv.put(Message.provider_value, recvMsg.value);

							Uri _uri = Uri.parse("content://"
									+ provider.AUTHORITY1);
							insert(_uri, cv);
							Log.d("Server", "Replication successful at " + myID
									+ "for key:" + recvMsg.key);

							replyMsg.destPort = recvMsg.originPort;
							replyMsg.originPort = mapPort.get(myHash);
							replyMsg.messageType = "replicateACK";
							replyMsg.originPort = mapPort.get(myHash);

							out.writeObject(replyMsg);
							Log.d("ServerThread", "" + Integer.parseInt(myID)
									+ " sent replicateACK to "
									+ replyMsg.destPort / 2);

						} else if (msgType.contentEquals("getREQ")
								|| msgType.contentEquals("getRepREQ")) {

							Log.d("Server: get", "Received " + msgType
									+ " for key " + recvMsg.key);
							getReply = new MessageWrapper();
							Cursor cur;
							if (msgType.contentEquals("getREQ"))
								cur = query(myUri, null, recvMsg.key, null,
										Message.provider_key + " ASC");
							else {
								Uri _uri = Uri.parse("content://"
										+ provider.AUTHORITY1);
								cur = query(_uri, null, recvMsg.key, null,
										Message.provider_key + " ASC");
							}
							boolean flag = false;
							cur.moveToFirst();
							String retKey = null, retValue = null;

							while (!cur.isLast()) {
								retKey = cur.getString(cur
										.getColumnIndex(Message.provider_key));
								retValue = cur
										.getString(cur
												.getColumnIndex(Message.provider_value));

								if (retKey.contentEquals(recvMsg.key)) {
									flag = true;
									break;
								}
								cur.moveToNext();
							}
							if (cur.getString(
									cur.getColumnIndex(Message.provider_key))
									.contentEquals(recvMsg.key)
									&& flag == false) {
								retKey = cur.getString(cur
										.getColumnIndex(Message.provider_key));
								retValue = cur
										.getString(cur
												.getColumnIndex(Message.provider_value));

							}
							// prepare getReply
							getReply.destPort = recvMsg.originPort;
							if (msgType.contentEquals("getREQ"))
								getReply.messageType = "getACK";
							else
								getReply.messageType = "getRepACK";
							getReply.key = recvMsg.key;
							getReply.value = retValue;
							getReply.originPort = mapPort.get(myHash);
							out.writeObject(getReply);
							Log.d("Server: get", "sent " + getReply.messageType
									+ " for key" + recvMsg.key);
						} else if (msgType.contentEquals("updateREQ")) {
							Log.d("Server", "Received updateREQ from "
									+ recvMsg.originPort + " for key "
									+ recvMsg.key);
							ContentValues cv = new ContentValues();
							cv.put(Message.provider_key, recvMsg.key);
							cv.put(Message.provider_value, recvMsg.value);

							update(myUri, cv, recvMsg.key, null);
							Log.d("Server", "updateREQ successfull for key "
									+ recvMsg.key);

							replyMsg.destPort = recvMsg.originPort;
							replyMsg.originPort = mapPort.get(myHash);
							replyMsg.messageType = "updateACK";
							replyMsg.originPort = mapPort.get(myHash);

							out.writeObject(replyMsg);
							Log.d("Server", "Update ACK sent");
						}

					}
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		};
		new Thread(runnable).start();

	}

	protected void ClientThread(final MessageWrapper sendMsg) {
		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					if (sendMsg.messageType.contentEquals("bootREQ")) {
						// calculate the keys which are mine
						bootMsg.keySet = new ArrayList<String>();

						for (int keyI = 0; keyI < 10; keyI++) {
							tempHash = genHash(keyI + "");
							String retHash = lookup(tempHash);
							if (retHash.contentEquals(myHash))
								bootMsg.keySet.add(keyI + "");
						}
						if (!bootMsg.keySet.isEmpty())
							Log.d("provider: initialize ", "my keySet is: "
									+ bootMsg.keySet);

						bootReply = new MessageWrapper();
						Log.d("provider", "Boot Setup STARTED");

						bootReply = ClientThread.send(sendMsg);
						String msgType = bootReply.messageType;

						if (msgType.contentEquals("null")) {
							Log.d("Boot Loader",
									"Succ1 is DOWN; trying to contact Succ2");
							bootMsg = new MessageWrapper();
							bootMsg.destPort = mapPort.get(succ2);
							bootMsg.originPort = mapPort.get(myHash);
							bootMsg.reqType = 2; // ask succ2
							bootMsg.messageType = "bootREQ";

							bootReply = ClientThread.send(bootMsg);
							if (bootReply.messageType.contentEquals("bootACK")) {
								GlobalData.isDataReplicated = true;
								if (bootReply.valueSet.size() > 0) {
									for (int index = 0; index < bootReply.valueSet
											.size(); index++) {
										ContentValues cv = new ContentValues();

										cv.put(Message.provider_key,
												bootReply.keySet.get(index));
										cv.put(Message.provider_value,
												bootReply.valueSet.get(index));
										// call insert
										insert(myUri, cv);
									}
									Log.d("provider: bootACK",
											"Copied data from succ2");
								} else
									Log.d("provider: BOOT",
											"No Data Sent by succ");
							}
						} else if (msgType.contentEquals("bootACK")) {
							GlobalData.isDataReplicated = true;
							if (bootReply.valueSet.size() > 0) {
								for (int index = 0; index < bootReply.valueSet
										.size(); index++) {
									ContentValues cv = new ContentValues();

									cv.put(Message.provider_key,
											bootReply.keySet.get(index));
									cv.put(Message.provider_value,
											bootReply.valueSet.get(index));
									// call insert
									insert(myUri, cv);
								}
								Log.d("provider: bootACK",
										"Copied data from succ1");
							} else
								Log.d("provider: BOOT", "No Data Sent by succ");
						} else if (msgType.contentEquals("bootNACK")) {
							Log.d("provider",
									"Nothing to read!! Boot Setup has FINISHED");
						} else {
							Log.d("provider", "Invalid Message");
						}
					} else if (sendMsg.messageType.contentEquals("putREQ")) {
						/* Send the remote put operation */
						putReply = new MessageWrapper();
						putReply = ClientThread.send(sendMsg);
						Log.d("Client", "Sent putREQ for key:" + sendMsg.key
								+ " to  " + sendMsg.destPort / 2);
						String msgType = putReply.messageType;

						if (msgType.contentEquals("putACK")) {
							Log.d("ClientThread",
									"Remote Put Operation Successfull for key "
											+ sendMsg.key);

						} else if (msgType.contentEquals("null")) {
							Log.d("Client: put", "Coordinator DOWN for key:"
									+ sendMsg.key);
							/* Lets try to insert in the successor */
							putReply = null;
							// lets ask succ1
							putReply = askSucc(sendMsg, sendMsg.destPort, 1,
									"putRepREQ");
							if (putReply.messageType.contentEquals("null")) {
								Log.d("Client: put", "put REQ for succ1 failed");
								putReply = askSucc(sendMsg,
										calcSucc(ring, sendMsg.destPort), 2,
										"putRepREQ");
								if (putReply.messageType.contentEquals("null")) {
									Log.d("Client: put",
											" Reached End of World!!!!!");
									Log.d("Client: put",
											"There is still hope, put successful at Succ-2 for key: "
													+ sendMsg.key);
								} else
									Log.d("Client: putrep",
											"Key value saved at succ2 for key: "
													+ sendMsg.key);
							}
						} else {
							Log.d("Client: putrep",
									"Key value saved at succ1 for key: "
											+ sendMsg.key);
						}
					} else if (sendMsg.messageType.contentEquals("getREQ")) {

						getReply = ClientThread.send(sendMsg);
						if (getReply.messageType.contentEquals("getACK")) {
							Log.d("Query: get", " Received getACK from "
									+ getReply.originPort + " for key"
									+ getReply.key);

							// initialize matrix cursor
							matCur.newRow().add(getReply.key)
									.add(getReply.value);

							synchronized (matCur) {
								matCur.notifyAll();
								Log.d("Server: get", "matCur is notified");
							}

						} else if (getReply.messageType.contentEquals("null")) {
							Log.d("Query: get ",
									"Coordinator DOWN, checking with Succ1");
							// ask the successor
							getReply = null;
							getReply = askSucc(sendMsg, mapPort.get(myHash), 1,
									"getRepREQ");
							if (getReply.messageType.contentEquals("getRepACK")) {
								matCur.newRow().add(getReply.key)
										.add(getReply.value);

								synchronized (matCur) {
									matCur.notifyAll();
									Log.d("Server: get", "matCur is notified");
								}

							} else if (getReply.messageType
									.contentEquals("null")) {
								Log.d("Client", "Succ1 is DOWN, asking succ2");
								getReply = null;
								getReply = askSucc(sendMsg,
										calcSucc(ring, mapPort.get(myHash)), 2,
										"getRepREQ");
								if (getReply.messageType
										.contentEquals("getRepACK")) {
									Log.d("Client", " getRepREQ successful");
									matCur.newRow().add(getReply.key)
											.add(getReply.value);

									synchronized (matCur) {
										matCur.notifyAll();
										Log.d("Server: get",
												"matCur is notified");
									}

								} else {
									Log.d("Client",
											"Error in reading the key/value");
								}
							}
						}
					} else if (sendMsg.messageType
							.contentEquals("replicateREQ")
							|| sendMsg.messageType.contentEquals("updateREQ")) {
						Log.d("Client: rep", "Replication Started at " + myID
								+ "for key " + sendMsg.key);

						replicateData(sendMsg, succ1, 1);
						replicateData(sendMsg, succ2, 2);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		new Thread(runnable).start();

	}

	protected MessageWrapper askSucc(MessageWrapper msg, int succ, int reqType,
			String msgType) throws NoSuchAlgorithmException, IOException {

		MessageWrapper askSucc = new MessageWrapper();
		MessageWrapper askSuccReply = new MessageWrapper();
		askSucc.destPort = calcSucc(ring, succ);
		askSucc.messageType = msgType;
		askSucc.key = msg.key;
		askSucc.originPort = mapPort.get(myHash);
		askSucc.reqType = reqType;

		askSuccReply = ClientThread.send(askSucc);
		Log.d("askSucc", "Sent " + msgType + " to " + (askSucc.destPort / 2));
		return askSuccReply;
	}

	protected String replicateData(MessageWrapper msg, String succ, int reqType)
			throws NoSuchAlgorithmException, IOException {
		replicateMsg = new MessageWrapper();
		replicateReply = new MessageWrapper();

		replicateMsg.destPort = mapPort.get(succ);
		replicateMsg.key = msg.key;
		replicateMsg.value = msg.value;
		replicateMsg.messageType = msg.messageType;
		replicateMsg.reqType = reqType;
		replicateMsg.originPort = mapPort.get(myHash);

		replicateReply = ClientThread.send(replicateMsg);
		return replicateReply.messageType;

	}

	protected String voteForMe(String succ, int reqType) throws IOException {
		voteMsg = new MessageWrapper();
		voteReply = new MessageWrapper();

		voteMsg.destPort = mapPort.get(succ);
		voteMsg.messageType = "voteREQ";
		voteMsg.reqType = reqType; // succ1
		voteMsg.originPort = mapPort.get(myHash);

		voteReply = ClientThread.send(voteMsg);
		Log.d("voteForMe", "Received vote: " + voteReply.messageType + " from "
				+ (voteReply.originPort / 2));
		return voteReply.messageType;
	}

	protected void intializeAll() {
		// get the emulator port for this Content Provider
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		myID = tel.getLine1Number()
				.substring(tel.getLine1Number().length() - 4);

		portArray.add(11108);
		portArray.add(11112);
		portArray.add(11116);
		portArray.add(11120);
		portArray.add(11124);

		succ1 = null;
		succ2 = null;

		try {
			myHash = genHash(myID);
			// calculate the structure of ring and reverse index of hash to port
			// numbers
			for (int i = 0; i < portArray.size(); i++) {
				tempHash = genHash((portArray.get(i) / 2) + "");
				ring.add(tempHash);
				mapPort.put(tempHash, portArray.get(i));
			}
			Log.d("provider: initialize", "Contents of ring are:" + ring);

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		calcPrefList(ring);

	}

	// Method to calculate the Preference List for current node
	protected void calcPrefList(List<String> ring) {

		Collections.sort(ring);
		int size = ring.size();
		int index = ring.indexOf(myHash);

		if ((index + 1) > size - 1) {
			succ1 = ring.get(0);
			succ2 = ring.get(1);
		} else if ((index + 1) > size - 2) {
			succ1 = ring.get(index + 1);
			succ2 = ring.get(0);
		} else {
			succ1 = ring.get(index + 1);
			succ2 = ring.get(index + 2);
		}

		System.out.println("Current entries in ring: " + ring);
		System.out.println("My " + myID + " succ1 is: " + succ1
				+ "& succ2 is: " + succ2);
	}

	// Method used to calculate the next successor of the destNode
	protected int calcSucc(List<String> ring, int devID)
			throws NoSuchAlgorithmException {

		String devHash = genHash("" + (devID / 2));
		Collections.sort(ring);
		int size = ring.size();
		int index = ring.indexOf(devHash);

		if ((index + 1) > size - 1)
			return mapPort.get(ring.get(0));
		else
			return mapPort.get(ring.get(index + 1));

	}

	// Method called to check if the key is local or remote
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

	// Generate hash value for each provider
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	static {
		UriCheck = new UriMatcher(UriMatcher.NO_MATCH);
		UriCheck.addURI(AUTHORITY, tableName, messages);

		msgProjMap = new HashMap<String, String>();
		msgProjMap.put(Message.MSG_ID, Message.MSG_ID);
		msgProjMap.put(Message.provider_key, Message.provider_key);
		msgProjMap.put(Message.provider_value, Message.provider_value);
	}

}