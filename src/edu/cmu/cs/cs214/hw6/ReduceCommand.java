package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Runs the reduce command
 * 
 * @author Akhil Prakash
 */
public class ReduceCommand extends WorkerCommand
{

	private static final long serialVersionUID = 7911239074195060332L;
	private final ReduceTask reduceTask;
	private static final String TAG = "Reduce Command";
	private final String name;

	public ReduceCommand(ReduceTask reduceTask, String name)
	{
		this.reduceTask = reduceTask;
		this.name = name;
	}

	@Override
	public void run()
	{
		Map<String, List<String>> wordToCount = new HashMap<String, List<String>>();

		String file = WorkerStorage.getIntermediateResultsDirectory(name);
		File fileDirectory = new File(file);
		File[] kvPairFiles = fileDirectory.listFiles();
		for (File files : kvPairFiles)
		{
			try
			{
				//we only want the shuffle files
				if (files.getName().indexOf("Shuffle") == 0)
				{
					Scanner in = new Scanner(files);
					while (in.hasNextLine())
					{
						//parse out the key value pairs
						String line = in.nextLine();
						String [] split = line.split(",");
						String key = split[0].split("=")[1];
						String value = split[1].split("=")[1];
						//need to remove the ">" at the end of value
						value = value.substring(0, value.length() - 1);
						List<String> count = wordToCount.get(key);
						if (count == null)
						{
							List<String> temp = new ArrayList<String>();
							temp.add(value);
							wordToCount.put(key, temp);
						} 
						else
						{
							count.add(value);
							wordToCount.put(key, count);
						}
					}
					in.close();
				}
			} 
			catch (IOException e)
			{
				Log.e(TAG, "Some IO Exception", e);
			}
		}
		Log.i(TAG, "Finished summing up everything");
		try
		{
			Emitter reduceEmitter = new ReduceEmitter(name);
			for (Entry<String, List<String>> entries : wordToCount.entrySet())
			{
				reduceTask.execute(entries.getKey(), entries.getValue().iterator(), reduceEmitter);
			}
			Log.i(TAG, "Finished executing reduce task");
			String finalFile = WorkerStorage.getFinalResultsDirectory(name);
			File fileDir = new File(finalFile);
			File [] files = fileDir.listFiles();
			ObjectOutputStream toMaster = new ObjectOutputStream(getSocket().getOutputStream());
			for (File finalFiles : files)
			{
				Log.i(TAG, "Wrote to master final file location");
				toMaster.writeObject(finalFiles.getAbsolutePath());
			}
			toMaster.close();
		} 
		catch (IOException e)
		{
			Log.e(TAG, "IO Exception writting list of key value pairs", e);
		}
	}

}
