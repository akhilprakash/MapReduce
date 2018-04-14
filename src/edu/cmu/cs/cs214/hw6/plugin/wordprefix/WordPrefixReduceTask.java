package edu.cmu.cs.cs214.hw6.plugin.wordprefix;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;

/**
 * The reduce task for a word-prefix map/reduce computation.
 */
public class WordPrefixReduceTask implements ReduceTask
{
	private static final long serialVersionUID = 6763871961687287020L;

	@Override
	public void execute(String key, Iterator<String> values, Emitter emitter) throws IOException
	{
		Map<String, Integer> map = new ConcurrentHashMap<String, Integer> ();
		while (values.hasNext())
		{
			String v = values.next();
			Integer count = map.get(v);
			if (count == null)
			{
				map.put(v, 1);
			}
			else
			{
				map.put(v, count + 1);
			}
		}
		String maxWord = null;
		int maxCount = 0;
		for (Entry<String, Integer> entry : map.entrySet())
		{
			if (entry.getValue() > maxCount)
			{
				maxCount = entry.getValue();
				maxWord = entry.getKey();
			}
		}
		emitter.emit(key, maxWord);
	}

}
