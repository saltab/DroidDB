package edu.buffalo.cse.cse486_586.simpledynamo;

/* Distributed System - Project 3: Replicated Key-Value Storage
 * Group: PentaDroid
 * Submitted by Saurabh Talbar
 */

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	private Button butPut1;
	private Button butPut2;
	private Button butPut3;
	private Button butGet;
	private Button butDump;
	private Button butExit;

	private TextView textOut;

	/** Called when the activity is first created. */
	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.main);

		butPut1 = (Button) findViewById(R.id.buttonPut1);
		butPut2 = (Button) findViewById(R.id.buttonPut2);
		butPut3 = (Button) findViewById(R.id.buttonPut3);
		butGet = (Button) findViewById(R.id.buttonGet);
		butDump = (Button) findViewById(R.id.buttonDump);
		butExit = (Button) findViewById(R.id.buttonExit);
		textOut = (TextView) findViewById(R.id.textOutput);

		butPut1.setOnClickListener(put1);
		butPut2.setOnClickListener(put2);
		butPut3.setOnClickListener(put3);
		butGet.setOnClickListener(get);
		butDump.setOnClickListener(dump);
		butExit.setOnClickListener(exitListener);

	}

	private OnClickListener put1 = new OnClickListener() {
		@Override
		public void onClick(View v) {

			GlobalData.flag = true;
			// GlobalData.seqNo = 0;
			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				insertCV("" + GlobalData.seqNo, "TEST1" + GlobalData.seqNo);
			}

		}

	};

	private OnClickListener put2 = new OnClickListener() {
		@Override
		public void onClick(View v) {

			GlobalData.flag = true;
			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++) {
				insertCV("" + GlobalData.seqNo, "TEST2" + GlobalData.seqNo);
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	};

	private OnClickListener put3 = new OnClickListener() {
		@Override
		public void onClick(View v) {

			GlobalData.flag = true;
			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				insertCV("" + GlobalData.seqNo, "TEST3" + GlobalData.seqNo);
			}

		}
	};

	private OnClickListener get = new OnClickListener() {
		@Override
		public void onClick(View v) {
			StringBuffer buffer;

			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++) {
				buffer = query("" + GlobalData.seqNo, 0);
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				textOut.append(buffer);
			}

		}

	};

	private OnClickListener dump = new OnClickListener() {
		@Override
		public void onClick(View v) {
			query(null, 1);
		}

	};

	private OnClickListener exitListener = new OnClickListener() {
		@Override
		public void onClick(View v) {
			// ServerThread.stopServer();
			Log.d("DroidChatActivity", "S: Server Stopped");
			System.exit(0);
		}
	};

	public void insertCV(String key, String message) {
		try {

			// generate appropriate Uri
			Uri uri = Uri
					.parse("content://" + provider.AUTHORITY + "/MessageT");
			if (uri == null)
				throw new IllegalArgumentException();
			ContentValues valuesCV = new ContentValues();
			valuesCV.put(Message.provider_key, key);
			valuesCV.put(Message.provider_value, message);

			// insert into ContentProvider i.e MessageProvider for my app
			getContentResolver().insert(uri, valuesCV);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public StringBuffer query(String key, int _case) {
		StringBuffer buff = new StringBuffer();
		try {
			boolean flag = false;
			Uri uri = Uri
					.parse("content://" + provider.AUTHORITY + "/MessageT");
			Cursor cur;
			if (_case == 0) {
				// for "get" operation
				cur = getContentResolver().query(uri, null, key, null,
						Message.provider_key + " ASC");
			} else {
				// for "dump" operation
				cur = getContentResolver().query(uri, null, null, null,
						Message.provider_key + " ASC");

			}
			Log.d("SimpleDHTActivity", "Received Some Cursor");

			if (cur.getCount() > 0) {
				cur.moveToFirst();

				int keyColumn = cur.getColumnIndex(Message.provider_key);
				int valueColumn = cur.getColumnIndex(Message.provider_value);
				if (_case == 0) {
					while (!cur.isLast()) {
						if (cur.getString(keyColumn).contentEquals(key)) {
							buff.append("		" + cur.getString(keyColumn)
									+ "         	" + cur.getString(valueColumn)
									+ "\n");
							flag = true;
							Log.d("Activity", "Gte operation for key:" + key
									+ " successful");
							break;
						}
						cur.moveToNext();
					}
					if (cur.getString(keyColumn).contentEquals(key)
							&& flag == false)
						buff.append("		" + cur.getString(keyColumn)
								+ "         	" + cur.getString(valueColumn)
								+ "\n");

				} else {
					while (!cur.isLast()) {
						buff.append("		" + cur.getString(keyColumn)
								+ "         	" + cur.getString(valueColumn)
								+ "\n");
						cur.moveToNext();
					}
					buff.append("		" + cur.getString(keyColumn) + "         	"
							+ cur.getString(valueColumn) + "\n");
					textOut.append("" + Message.provider_key + ""
							+ Message.provider_value + "\n");
					textOut.append(buff);

				}
			} else {
				Log.d("Activity: Display Button", "Empty");
				buff.append("No Entry Yet!");
			}

			cur.close();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
		return buff;
	}
}