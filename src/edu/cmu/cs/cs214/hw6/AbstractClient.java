package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.cmu.cs.cs214.hw6.plugin.wordcount.WordCountClient;
import edu.cmu.cs.cs214.hw6.plugin.wordprefix.WordPrefixClient;
import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 */
public abstract class AbstractClient
{
	private final String mMasterHost;
	private final int mMasterPort;
	private static final String TAG = "Abstract Client";

	/**
	 * The {@link AbstractClient} constructor.
	 *
	 * @param masterHost
	 *                The host name of the {@link MasterServer}.
	 * @param masterPort
	 *                The port that the {@link MasterServer} is listening
	 *                on.
	 */
	public AbstractClient(String masterHost, int masterPort)
	{
		mMasterHost = masterHost;
		mMasterPort = masterPort;
	}

	protected abstract MapTask getMapTask();

	protected abstract ReduceTask getReduceTask();

	public void execute()
	{
		final MapTask mapTask = getMapTask();
		final ReduceTask reduceTask = getReduceTask();
		//Make a connection to the master server
		Socket connectionToMaster = null;
		try
		{
			connectionToMaster = new Socket(mMasterHost, mMasterPort);
		}
		catch (IOException e)
		{
			Log.e(TAG, "Unable to connect to Master Server", e);
			return;
		}
		//send the map and the reduce task to the master server
		try
		{
			ObjectOutputStream out = new ObjectOutputStream(connectionToMaster.getOutputStream());
			Log.i(TAG, "Wrote map task to master server");
			out.writeObject(mapTask);
			Log.i(TAG, "Wrote Reduce task to master server");
			out.writeObject(reduceTask);
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Unable to get Output Stream of Master Server", e);
			try
			{
				connectionToMaster.close();
			} 
			catch (IOException e1)
			{
				//dont care since we could not write to the master
			}
			return;
		}
		
		//wait for master server to send back results
		try
		{
			String file = "";
			while (! file.equals("done"))
			{
				ObjectInputStream input = new ObjectInputStream(connectionToMaster.getInputStream());	
				file = (String) input.readObject();
				System.out.println(file);
			}
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Could not get input from master server", e);
		}
		catch (ClassNotFoundException e)
		{
			Log.e(TAG, "Class not Found", e);
		}
		
		try
		{
			connectionToMaster.close();
		} 
		catch (IOException e)
		{
			//Don't care since we are done
		}
	}

}
