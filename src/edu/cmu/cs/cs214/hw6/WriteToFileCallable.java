package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.KeyValuePair;

public class WriteToFileCallable implements Callable<Boolean>
{
	private final int port;
	private final String name;
	private final String hostName;
	private final ArrayList<KeyValuePair> keyValuePairs;
	
	public WriteToFileCallable(String hostName, int port, String name, ArrayList<KeyValuePair> keyValuePairs)
	{
		this.hostName = hostName;
		this.port = port;
		this.name = name;
		this.keyValuePairs = keyValuePairs;
	}

	@Override
	public Boolean call() throws Exception
	{
		Socket socket = new Socket(hostName, port);
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(new WriteToFileCommand(keyValuePairs, name));
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		Boolean b =  (Boolean) in.readObject();
		socket.close();
		return b;
	}

}
