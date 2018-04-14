package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.cs.cs214.hw6.util.KeyValuePair;
import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Goes through all the intermediate results files and sends the key value pairs to the appropriate worker server
 * 
 * @author Akhil Prakash
 */
public class ShuffleCommand extends WorkerCommand
{

	private static final long serialVersionUID = 8662259040065758020L;
	private static final String TAG = "Shuffle Command";
	private final String name;
	private final List<WorkerInfo> wInfo;
	private ExecutorService mExecutor;

	public ShuffleCommand(String name, List<WorkerInfo> wInfo)
	{
		this.name = name;
		this.wInfo = wInfo;
	}

	@Override
	public void run()
	{
		mExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		Log.i(TAG, "At beginning");
		String interDir = WorkerStorage.getIntermediateResultsDirectory(name);
		File intermediate = new File(interDir);
		File[] allIntermediateFile = intermediate.listFiles();
		Map<Integer, ArrayList<KeyValuePair>> map = new ConcurrentHashMap<Integer, ArrayList<KeyValuePair>> ();
		for (File file : allIntermediateFile)
		{
			try
			{
					Scanner input = new Scanner(file);
					while(input.hasNextLine())
					{
						//parse the key value pair
						String line = input.nextLine();
						String [] split = line.split(",");
						String key = split[0].split("=")[1];
						String value = split[1].split("=")[1];
						//need to remove the ">" at the end of the value
						value = value.substring(0, value.length() -1);
						KeyValuePair pair = new KeyValuePair(key, value);
						int hash = pair.hashCode() % wInfo.size();
						if (hash < 0)
						{
							hash += wInfo.size();
						}
						ArrayList<KeyValuePair> kvps = map.get(hash);
						if (kvps != null)
						{
							kvps.add(pair);
							map.put(hash, kvps);
						}
						else
						{
							ArrayList<KeyValuePair> pairs = new ArrayList<KeyValuePair> ();
							pairs.add(pair);
							map.put(hash, pairs);
						}
					}
					input.close();
					Log.i(TAG, "Done shuffling");
			}
			catch (IOException e)
			{
				Log.e(TAG, "Could not open intermediate result file", e);
			}
		}
		
		try
		{
			List<WriteToFileCallable> commands = new ArrayList<WriteToFileCallable> ();
			//send the list of key value pairs to each worker
			for (int i = 0; i < wInfo.size(); i++)
			{
				commands.add(new WriteToFileCallable(wInfo.get(i).getHost(), 
					wInfo.get(i).getPort(), wInfo.get(i).getName(),
					map.get(i)));
			}
			List<Future<Boolean>> result = mExecutor.invokeAll(commands);
			Log.i(TAG, "Invoked Write to file callable command");
			for (Future<Boolean> bool : result)
			{
				if ( bool.get().equals(Boolean.FALSE))
				{
					//something bad happened
				}
			}
			Log.i(TAG, "Wrote the key Value Pairs to other workers");
			ObjectOutputStream toMaster = new ObjectOutputStream(getSocket().getOutputStream());
			toMaster.writeObject(Boolean.TRUE);
			Log.i(TAG, "Tried to tell master that I am done with shuffle");
			toMaster.close();
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Problem writing to master", e);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		} 
		catch (ExecutionException e)
		{
			e.printStackTrace();
		}
	}
}
