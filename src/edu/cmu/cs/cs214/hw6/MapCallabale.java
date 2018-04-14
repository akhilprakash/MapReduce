package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Writes the map command to worker server and then reads the boolean which is written in the map command
 * 
 * @author Akhil Prakash
 */
public class MapCallabale implements Callable<Boolean>
{

	private final Socket socket;
	private final MapTask mapTask;
	private final String name;
	private final List<Partition> partition;
	private static final String TAG = "Map Callable"; 
	
	public MapCallabale(Socket socket, MapTask mapTask, String name, List<Partition> partition)
	{
		this.socket = socket;
		this.mapTask = mapTask;
		this.name = name;
		this.partition = partition;
	}
	
	@Override
	public Boolean call() throws Exception
	{
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(new MapCommand(mapTask, name, partition));
		Log.i(TAG, "Reading in ackowdlgement that map finished for one worker");
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		return (Boolean) in.readObject();
	}

}
