package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Deletes all the files in the intermediate results directory
 * 
 * @author Akhil Prakash
 */
public class DeleteFilesCommand extends WorkerCommand
{

	private static final long serialVersionUID = -8905117666581691796L;
	private final String name;
	private static final String TAG = "Delete Files Command";
	
	public DeleteFilesCommand(String name)
	{
		this.name = name;
	}
	
	
	@Override
	public void run()
	{
		String file = WorkerStorage.getIntermediateResultsDirectory(name);
		File fileDirectory = new File(file);
		File [] allFiles = fileDirectory.listFiles();
		for (File delete : allFiles)
		{
			delete.delete();
		}
		try
		{
			ObjectOutputStream toMaster = new ObjectOutputStream(getSocket().getOutputStream());
			toMaster.writeObject(Boolean.TRUE);
			toMaster.close();
			Log.i(TAG, "Tried to tell master that I am done deleting temporary files");
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Problem writing to master", e);
		}
			
		
	}

}
