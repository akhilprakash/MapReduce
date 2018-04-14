package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import edu.cmu.cs.cs214.hw6.util.KeyValuePair;
import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Writes the list of key value pairs from each worker to a file.
 * 
 * @author Akhil Prakash
 */
public class WriteToFileCommand extends WorkerCommand
{

	private static final long serialVersionUID = -4542719357926545169L;
	private final ArrayList<KeyValuePair> pairs;
	private final String name;
	private static final String TAG = "Write to File Command";
	
	public WriteToFileCommand(ArrayList<KeyValuePair> pairs, String name)
	{
		this.pairs = pairs;
		this.name = name;
	}
	
	@Override
	public void run()
	{
		if (pairs != null && ! pairs.isEmpty())
		{
			Log.i(TAG, "writing to new file");
			String file = WorkerStorage.getIntermediateResultsDirectory(name);
			File fileDirectory = new File(file);
			try
			{
				String fileName = "Shuffle" + Long.toString(System.currentTimeMillis());
				File toWrite = File.createTempFile(fileName, ".txt", fileDirectory);
				FileOutputStream output = new FileOutputStream(toWrite);
				for (KeyValuePair p : pairs)
				{
					output.write((p.toString() + "\n").getBytes());
				}
				output.close();
				Log.i(TAG, "Finished writing one file");
			}
			catch (IOException e)
			{
				Log.e(TAG, "Could not make temp file", e);
			}
		}
		//need to write back to worker who sent me this command 
		try
		{
			Log.i(TAG, "About to write true after writing one file");
			ObjectOutputStream out = new ObjectOutputStream(getSocket().getOutputStream());
			out.writeObject(Boolean.TRUE);
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

}
