package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import edu.cmu.cs.cs214.hw6.util.KeyValuePair;
import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Called in the reduce task class. Make a file in the final results directory. Write to this file.
 * 
 * @author Akhil Prakash
 */
public class ReduceEmitter implements Emitter
{
	private FileOutputStream output = null;
	private static final String TAG = "Reduce Emitter";

	public ReduceEmitter(String name) throws IOException
	{
		String file = WorkerStorage.getFinalResultsDirectory(name);
		File fileDirectory = new File(file);
		try
		{
			String fileName = file + Long.toString(System.currentTimeMillis());
			File toWrite = File.createTempFile(fileName, ".txt", fileDirectory);
			output = new FileOutputStream(toWrite);
		}
		catch (IOException e)
		{
			Log.e(TAG, "Could not make temp file", e);
			throw e;
		}
	}
	
	
	@Override
	public void close() throws IOException
	{
		output.close();

	}

	@Override
	public void emit(String key, String value) throws IOException
	{
		KeyValuePair pair = new KeyValuePair(key, value);
		try
		{
			output.write((pair.toString() + "\n").getBytes());
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Unable to write " + pair.toString(), e);
		}

	}

}
