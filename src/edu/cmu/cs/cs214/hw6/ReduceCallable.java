package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

/**
 * Calls the reduce task and then read the file path for the final results file and then write the final
 * results file path to the abstract client  
 * 
 * @author Akhil Prakash
 *
 */
public class ReduceCallable implements Callable<Boolean>
{

	private final ReduceTask reduceTask;
	private final String name;
	private final Socket socket;
	private final Socket abstractClient;
	
	public ReduceCallable(Socket socket, ReduceTask reduceTask, String name, Socket abstractClient)
	{
		this.socket = socket;
		this.reduceTask = reduceTask;
		this.name = name;
		this.abstractClient = abstractClient;
	}
	
	@Override
	public Boolean call() throws Exception
	{
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(new ReduceCommand(reduceTask, name));
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		String filePath = (String) in.readObject();
		ObjectOutputStream o = new ObjectOutputStream(abstractClient.getOutputStream());
		o.writeObject(filePath);
		return Boolean.TRUE;
	}

}
