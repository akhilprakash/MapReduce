package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.StaffUtils;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 *
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the locations of the final results files back to the client.</li>
 * </ol>
 */
public class MasterServer extends Thread
{
	private static final String TAG = "Master Server";
	private final int mPort;
	private final List<WorkerInfo> mWorkers;
	private final List<Socket> connectionToWorkers;
	private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors();

	private final ExecutorService mExecutor;
	private final List<WorkerInfo> failedWorkers;

	/**
	 * The {@link MasterServer} constructor.
	 *
	 * @param masterPort The port to listen on.
	 * @param workers Information about each of the available workers in the
	 *                system.
	 */
	public MasterServer(int masterPort, List<WorkerInfo> workers)
	{
		mPort = masterPort;
		mWorkers = workers;
		connectionToWorkers = new ArrayList<Socket>(workers.size());
		mExecutor = Executors.newFixedThreadPool(POOL_SIZE);
		// Make all the sockets to the worker servers so we can send them tasks
		for (WorkerInfo worker : mWorkers)
		{
			try
			{
				connectionToWorkers.add(new Socket(worker.getHost(), worker.getPort()));
			} 
			catch (IOException e)
			{
				Log.e(TAG, "Master could not connect to worker servers", e);
			}
		}
		failedWorkers = new ArrayList<WorkerInfo> ();
	}

	/**
	 * Given a partition return all the workers who have that partition
	 * 
	 * @param p a specific partition
	 * 
	 * @return list of worker infos who have this partition.
	 *         NOTE: return value is never null. can return empty list.
	 */
	private List<WorkerInfo> getWorkers(Partition p)
	{
		List<WorkerInfo> workers = new ArrayList<WorkerInfo>();
		for (WorkerInfo wInfo : mWorkers)
		{
			List<Partition> partitions = wInfo.getPartitions();
			if (partitions.contains(p))
			{
				workers.add(wInfo);
			}
		}
		return workers;
	}

	/**
	 * Determines if a partition is already in the map of workers to
	 * partitions
	 * 
	 * @param p a specific partition
	 * @param map the current map of workers to partitions
	 * 
	 * @return true if p is in map false otherwise
	 */
	private boolean isInMap(Partition p, Map<WorkerInfo, List<Partition>> map)
	{
		for (List<Partition> partitions : map.values())
		{
			if (partitions.contains(p))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Given a map and a list of workers determine which worker has the
	 * least number of partitions assigned to him
	 * 
	 * @param workers a set of workers
	 * @param map the map of workers to partitions
	 * 
	 * @return the worker which has the least number of partitions assigned
	 *         to it. Null if workers is empty.
	 *         
	 * NOTE: Does not modify the map
	 */
	private WorkerInfo findMin(List<WorkerInfo> workers, Map<WorkerInfo, List<Partition>> map)
	{
		int min = Integer.MAX_VALUE;
		WorkerInfo useMe = null;
		for (WorkerInfo wInfo : workers)
		{
			List<Partition> partitions = map.get(wInfo);
			int size = partitions == null ? 0 : partitions.size();
			if (size < min)
			{
				min = size;
				useMe = wInfo;
			}
		}
		return useMe;
	}

	/**
	 * Here we partition the partitions to specific workers We have a greedy
	 * algorithm where we just go through all the partitions and which every
	 * worker has the least number of partitions assigned to it we assign
	 * that partition to it
	 * 
	 * @return a map from worker to the list of partitions which the worker
	 *         has in its directory and also there is no more duplicate
	 *         stuff.
	 */
	private Map<WorkerInfo, List<Partition>> partitionPartitions()
	{
		Map<WorkerInfo, List<Partition>> workerToPartition = new HashMap<WorkerInfo, List<Partition>>();
		// Need to split the partition in a good way so don't duplicate
		for (WorkerInfo wInfo : mWorkers)
		{
			List<Partition> partitions = wInfo.getPartitions();
			for (Partition p : partitions)
			{
				List<WorkerInfo> workers = getWorkers(p);
				if (!isInMap(p, workerToPartition))
				{
					WorkerInfo workerInfo = findMin(workers, workerToPartition);
					List<Partition> partitionInMap = workerToPartition.get(workerInfo);
					if (partitionInMap == null)
					{
						List<Partition> part = new ArrayList<Partition>();
						part.add(p);
						workerToPartition.put(workerInfo, part);
					} 
					else
					{
						partitionInMap.add(p);
						workerToPartition.put(workerInfo, partitionInMap);
					}
				}
			}
		}
		return workerToPartition;
	}

	@Override
	public void run()
	{
		/*
		 * Wait for a map reduce task. which means need to wait for
		 * someone to connect to the socket that I make
		 */
		ServerSocket serverSocket = null;
		try
		{
			serverSocket = new ServerSocket(mPort);
		} 
		catch (IOException e)
		{
			Log.e(TAG, "Could not open server socket on port " + mPort + ".", e);
			return;
		}

		Log.i(TAG, "Listening for incoming commands on port " + mPort + ".");

		while (true)
		{
			try
			{
				Socket abstractClientSocket = serverSocket.accept();
				ObjectInputStream input = new ObjectInputStream(abstractClientSocket.getInputStream());
				List<DeleteFilesCallable> deleteCallable = new ArrayList<DeleteFilesCallable> ();
				//delete any left over files from previous run
				for (int i = 0; i < connectionToWorkers.size(); i++)
				{
					if (! failedWorkers.contains(mWorkers.get(i)))
					{
						deleteCallable.add(new DeleteFilesCallable(new Socket(mWorkers.get(i).getHost(),
							mWorkers.get(i).getPort()), mWorkers.get(i).getName()));
					}
				}
				List<Future<Boolean>> result = mExecutor.invokeAll(deleteCallable);
				for (Future<Boolean> bool : result)
				{
					if (bool.get().equals(Boolean.FALSE))
					{
						// something bad happened
					}
				}
				MapTask mapTask = (MapTask) input.readObject();
				// need to send the mapTask to different worker servers
				Map<WorkerInfo, List<Partition>> workerInfoToPartition = partitionPartitions();
				List<MapCallabale> mapCall = new ArrayList<MapCallabale>();
				for (int i = 0; i < connectionToWorkers.size(); i++)
				{
					if (! failedWorkers.contains(mWorkers.get(i)))
					{
						mapCall.add(new MapCallabale(connectionToWorkers.get(i), mapTask,
							mWorkers.get(i).getName(), workerInfoToPartition.get(mWorkers.get(i))));
					}
				}
				result = mExecutor.invokeAll(mapCall);
				Log.i(TAG, "Finished telling workers to map");
				/*
				 * Have this weird control flow because we still need to call get on every
				 * future if one of the workers fails so that the other workers finish and then I 
				 * can resend then the failed map job
				 */
				boolean isMapOver = false;
				while(! isMapOver)
				{
					isMapOver = true;
					int j = 0;
					while (j < result.size())
					{
						try 
						{
							Future<Boolean> bool = result.get(j);
							if (bool.get().equals(Boolean.FALSE))
							{
								// something bad happened
							}
							j++;
						} 
						catch (InterruptedException e)
						{
							isMapOver = false;
							Log.e(TAG, "Interrepted during map", e);
							break;
						}
						catch (ExecutionException e)
						{
							//some job has failed
							isMapOver = false;
							failedWorkers.add(mWorkers.get(j));
							List<Partition> failed = mWorkers.get(j).getPartitions();
							workerInfoToPartition.clear();
							//redistribute the partitions of the failed server
							for (Partition p : failed)
							{
								List<WorkerInfo> workers = getWorkers(p);
								WorkerInfo assign = findMin(workers, workerInfoToPartition);
								if (assign != null)
								{
									List<Partition> partitions = workerInfoToPartition.get(assign);
									if (partitions == null)
									{
										List<Partition> parts = new ArrayList<Partition> ();
										parts.add(p);
										workerInfoToPartition.put(assign, parts);
									}
									else
									{
										partitions.add(p);
										workerInfoToPartition.put(assign, partitions);
									}
								}
							}
						}
					}
					/*
					 * Now we have gotten all the map workers that failed on this particular try
					 * of running the map command
					 * 
					 * And we have also redistributed all the partitions of the failed workers
					 */
					if (! failedWorkers.isEmpty())
					{
						mapCall.clear();
						for (int i = 0; i < connectionToWorkers.size(); i++)
						{
							if (! failedWorkers.contains(mWorkers.get(i)))
							{
								mapCall.add(new MapCallabale(connectionToWorkers.get(i), mapTask,
									mWorkers.get(i).getName(), workerInfoToPartition.get(mWorkers.get(i))));
							}
						}
						//run the map task again
						result = mExecutor.invokeAll(mapCall);
					}
				}
				
				ReduceTask reduceTask = (ReduceTask) input.readObject();
				// All the map Tasks have finished
				Log.i(TAG, "All map tasks have finished");
				List<ShuffleCallable> shuffle = new ArrayList<ShuffleCallable>();
				for (int i = 0; i < connectionToWorkers.size(); i++)
				{
					if (! failedWorkers.contains(mWorkers.get(i)))
					{
						shuffle.add(new ShuffleCallable(new Socket(mWorkers.get(i).getHost(),
							mWorkers.get(i).getPort()), Collections.unmodifiableList(mWorkers),
							mWorkers.get(i).getName()));
					}
				}
				result = mExecutor.invokeAll(shuffle);
				Log.i(TAG, "Invoked shuffle");
				for (int i = 0; i < result.size(); i++)
				{
					try 
					{
						result.get(i).get();
					}
					catch (ExecutionException | InterruptedException e)
					{
						// something bad happened
						failedWorkers.add(mWorkers.get(i));
						Log.e(TAG, "Shuffle failed");
					}
				}
				List<ReduceCallable> reduce = new ArrayList<ReduceCallable>();
				Log.i(TAG, "Invoked reduce");
				for (int i = 0; i < connectionToWorkers.size(); i++)
				{
					if (! failedWorkers.contains(mWorkers.get(i)))
					{
						reduce.add(new ReduceCallable(new Socket(mWorkers.get(i).getHost(),
							mWorkers.get(i).getPort()), reduceTask, mWorkers.get(i).getName(),
							abstractClientSocket));
					}
				}
				result = mExecutor.invokeAll(reduce);
				for (int i = 0; i < result.size(); i++)
				{
					try 
					{
						result.get(i).get();
					}
					catch (ExecutionException | InterruptedException e)
					{
						// something bad happened
						failedWorkers.add(mWorkers.get(i));
						Log.e(TAG, "reduce failed");
					}
				}
				Log.i(TAG, "Writing done to abstract client");
				ObjectOutputStream out = new ObjectOutputStream(abstractClientSocket.getOutputStream());
				out.writeObject("done");
				Log.i(TAG, "Done with map reduce");
			}
			catch (IOException | ClassNotFoundException e)
			{
				Log.e(TAG, "Error while listening for incoming connections.", e);
				break;
			}
			catch (InterruptedException e)
			{
				Log.e(TAG, "Interrupt in futures", e);
				break;
			}
			catch (ExecutionException e)
			{
				Log.e(TAG, "Some worker failed", e);
				break;
			}
		}

		Log.i(TAG, "Shutting down...");

		try
		{
			for (Socket o : connectionToWorkers)
			{
				o.close();
			}
			serverSocket.close();
		} 
		catch (IOException e)
		{
			// Ignore because we're about to exit anyway.
		}
	}

	/********************************************************************/
	/***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
	/********************************************************************/

	/**
	 * Starts the master server on a distinct port. Information about each
	 * available worker in the distributed system is parsed and passed as an
	 * argument to the {@link MasterServer} constructor. This information
	 * can be either specified via command line arguments or via system
	 * properties specified in the <code>master.properties</code> and
	 * <code>workers.properties</code> file (if no command line arguments
	 * are specified).
	 */
	public static void main(String[] args)
	{
		StaffUtils.makeMasterServer(args).start();
	}

}
