package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.List;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Runs the map task
 * 
 * @author Akhil Prakash
 */
public class MapCommand extends WorkerCommand
{

	private static final long serialVersionUID = -1730273105840549773L;
	private static final String TAG = "Map Command";
	private final MapTask mapTask;
	private final String name;
	private final List<Partition> partitions;
	
	public MapCommand(MapTask mapTask, String name, List<Partition> partitions)
	{
		this.mapTask = mapTask;
		this.name = name;
		this.partitions = partitions;
	}
	
	@Override
	public void run()
	{
		try
		{
			//go through all the partitions and execute the map task on each partition
			MapEmitter emitter = new MapEmitter(name);
			if (partitions != null)
			{
				for (Partition p : partitions)
				{
					Iterator<File> inputs = p.iterator();
					while (inputs.hasNext())
					{
						mapTask.execute(new FileInputStream(inputs.next()), emitter);
					}
				}
			}
			Log.i(TAG, "Finished doing map task");
		}
		catch (IOException e)
		{
			Log.e(TAG, "Some IO Exception", e);
			
		}
		try
		{
			ObjectOutputStream toMaster = new ObjectOutputStream(getSocket().getOutputStream());
			toMaster.writeObject(Boolean.TRUE);
			toMaster.close();
			Log.i(TAG, "Tried to tell master that I am done with map");
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Problem writing to master", e);
		}
	}

}