package yongs.temp.partitioner;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class SalaryUpdatePartitioner implements Partitioner {
	private Logger logger = LoggerFactory.getLogger(SalaryUpdatePartitioner.class);
	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<>(gridSize);
		
		for(int i=0; i<gridSize; i++) {
			ExecutionContext value = new ExecutionContext();
			value.putInt("no", i);
			
			map.put("partition"+i, value);
		}
		logger.debug("ExecutionContext ==>" + map);
		return map;
	}

}
