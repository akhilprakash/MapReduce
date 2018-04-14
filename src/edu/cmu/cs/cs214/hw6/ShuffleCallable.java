package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Writes the shuffle command to a worker server and then reads the boolean from the worker server
 * 
 * @author Akhil Prakash
 */
public class ShuffleCallable implements Callable<Boolean>
{
	
	private final Socket socket;
	private final List<WorkerInfo> mWorkers;
	private final String name;
	
	public ShuffleCallable(Socket socket, List<WorkerInfo> mWorkers, String name)
	{
		this.socket = socket;
		this.mWorkers = mWorkers;
		this.name = name;
	}
	

	@Override
	public Boolean call() throws Exception
	{
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(new ShuffleCommand(name, mWorkers));
		
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		return (Boolean) in.readObject();
	}

}
