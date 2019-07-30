package com.dtdb.master.dispatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.dtdb.master.parser.IParser;
import com.dtdb.master.parser.QueryType;
import com.dtdb.master.rest.WorkerRequest;

@Component
public class WorkerDispatcher implements IDispatcher {

	private static final Logger logger = org.slf4j.LoggerFactory
			.getLogger(WorkerDispatcher.class);

	private @Autowired IParser parser;
	private @Autowired WorkerDetailsRepository workerRepository;
	private @Autowired EntityManager entityManager;

	@Override
	public String executeQuery(Map<QueryType, String> data) {
		ExecutorService service = Executors.newFixedThreadPool(5);
		String consolidationQuery = data.get(QueryType.CONSOLIDATION_QUERY);
		String finalQuery = data.get(QueryType.FINAL_QUERY);
		Iterator<WorkerDetails> workerIterator = workerRepository.findAll()
				.iterator();
		String result = "";

		List<Callable<String>> tasks = new ArrayList<Callable<String>>();
		try {
			System.out.println("----------------Calling Workers");
			while (workerIterator.hasNext()) {
				WorkerDetails worker = workerIterator.next();
				Callable<String> task = () -> {
					try {
						Long timestamp = System.currentTimeMillis();
						String url = "http://" + worker.getIpAddress() + ":"
								+ worker.getPort() + "/executeQuery";
						RestTemplate template = new RestTemplate();
						WorkerRequest request = new WorkerRequest();
						request.setConsolidationQuery(data
								.get(QueryType.CONSOLIDATION_QUERY));
						request.setDistributedQuery(data
								.get(QueryType.DISTRIBUTED_QUERY));
						logger.debug("Dispatching request to worker: "
								+ worker.getName());
						String workerResult = template.postForObject(url,
								request, String.class);
						logger.debug("Received response from worker : "
								+ worker.getName());
						System.out.println(" Worker - "+worker.getName()+" Took "+((System.currentTimeMillis()-timestamp)/1000)+" Seconds to complete");
						return workerResult;
					} catch (Exception e) {
						e.printStackTrace();
						throw new Exception("task interrupted", e);
					}
				};
				tasks.add(task);
			}
			List<Future<String>> results = service.invokeAll(tasks);
			List<String> resultJSONs = new ArrayList<String>();
			for (Future<String> future : results) {
				String resultSet = future.get();
				if(resultSet!=null && resultSet.trim().length()>0){
					resultJSONs.add(resultSet.replace("[", "").replace("]", ""));
				}
			}
			//if(tasks.size()>1){
			result = resultJSONs.toString();
			String groupBy = "";
			if(consolidationQuery.indexOf("GROUP BY")>0){
				groupBy = consolidationQuery.substring(consolidationQuery.indexOf("GROUP BY"));
			}
			result = "from (select json_array_elements(data) as rowdata from ( select cast('"+ result+"' as json) as data ) t)t";
			String cQuery = "select "+finalQuery+" from ( select "+consolidationQuery.replaceAll("VITREOS_REPLACER_STRING", result)+") row "+groupBy;
			result = cQuery;
			if(cQuery.endsWith("]")){
				cQuery = cQuery.substring(0, consolidationQuery.length()-1);
			}
			//System.out.println(cQuery);
			return cQuery;

			//String str = (String)entityManager.createNativeQuery(cQuery).getResultList().get(0);
			/*}else{
				return resultJSONs.toString();
			}*/
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return result;
	}
}
