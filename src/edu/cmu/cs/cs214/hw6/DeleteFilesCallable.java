package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Calls the delete files command 
 * @author Akhil Prakash
 *
 */
public class DeleteFilesCallable implements Callable<Boolean>
{

	private final Socket socket;
	private final String name;
	private static final String TAG = "Delete Files Callable"; 
	
	public DeleteFilesCallable(Socket socket, String name)
	{
		this.socket = socket;
		this.name = name;
	}
	
	@Override
	public Boolean call() throws Exception
	{
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(new DeleteFilesCommand(name));
		Log.i(TAG, "Reading in ackowdlgement that delete files finished for one worker");
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		return (Boolean) in.readObject();
	}

}
