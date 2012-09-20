package edu.buffalo.cse.cse486_586.simpledht;

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

public class SimpleDHTActivity extends Activity {

	private Button sendExit;
	private Button test1;
	private Button dumpClick;

	private TextView textOut;

	/** Called when the activity is first created. */
	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.main);

		// e.g local port = 5554...5562

		test1 = (Button) findViewById(R.id.buttonSend);
		sendExit = (Button) findViewById(R.id.buttonExit);
		dumpClick = (Button) findViewById(R.id.buttonDisp);
		textOut = (TextView) findViewById(R.id.textOutput);

		test1.setOnClickListener(test1Case);
		dumpClick.setOnClickListener(dump);
		sendExit.setOnClickListener(exitListener);
	}

	private OnClickListener test1Case = new OnClickListener() {
		@Override
		public void onClick(View v) {
			StringBuffer buffer;

			GlobalData.flag = true;
			// GlobalData.seqNo = 4;
			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++)
				insertCV("" + GlobalData.seqNo, "TEST" + GlobalData.seqNo);
			// Log.d("SimpleDHTActivity", "Inserted: " + GlobalData.seqNo+
			// " , TEST"+GlobalData.seqNo);
			// GlobalData.seqNo = 0;
			textOut.append("" + Message.provider_key + ""
					+ Message.provider_value + "\n");
			for (GlobalData.seqNo = 0; GlobalData.seqNo < 10; GlobalData.seqNo++) {
				buffer = query("" + GlobalData.seqNo,
						"TEST" + GlobalData.seqNo, 0);
				textOut.append(buffer);
			}
		}
	};

	private OnClickListener dump = new OnClickListener() {
		@Override
		public void onClick(View v) {

			/*
			 * Uri uri = Uri.parse("content://" + provider.AUTHORITY +
			 * "/MessageT"); Cursor cur = getContentResolver().query(uri, null,
			 * null, null, Message.provider_key + " ASC");
			 * 
			 * Log.d("SimpleDHTActivity","Received Cursor for Local Values"); if
			 * (cur.getCount() > 0) { StringBuffer buff = new StringBuffer();
			 * cur.moveToFirst(); int keyColumn =
			 * cur.getColumnIndex(Message.provider_key); int valueColumn =
			 * cur.getColumnIndex(Message.provider_value); while (!cur.isLast())
			 * { buff.append("		"+cur.getString(keyColumn) + "         	"+
			 * cur.getString(valueColumn) + "\n"); cur.moveToNext(); }
			 * buff.append("		"+cur.getString(keyColumn) + "         	"+
			 * cur.getString(valueColumn) + "\n"); textOut.append("" +
			 * Message.provider_key+"" + Message.provider_value + "\n");
			 * textOut.append(buff); } else { Log.d("Activity: Display Button",
			 * "Empty"); textOut.append("No Entry Yet!"); }
			 */
			query(null,null, 1);
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

	/* Insert Interface to Content Provider */
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

			/* Proceed with querying the value just inserted */

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public StringBuffer query(String key, String message, int _case) {
		StringBuffer buff = new StringBuffer();
		try {
			boolean flag = false;
			Uri uri = Uri
					.parse("content://" + provider.AUTHORITY + "/MessageT");
			Cursor cur;
			if (_case == 0) {
				cur = getContentResolver().query(uri, null, key, null,
						Message.provider_key + " ASC");
			} else {
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