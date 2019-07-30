package com.dtdb.worker.rest;

import javax.persistence.EntityManager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dtdb.worker.datasource.IVitreosDataSource;

@RestController
public class ExecutorService {

	private @Autowired IVitreosDataSource datasource;
	private @Autowired EntityManager entityManager;

	@RequestMapping(value = "/executeQuery", produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE, method=RequestMethod.POST)
	public String executeQuery(@RequestBody final WorkerRequest request) {
		datasource.initialize();
		String query = "select array_to_json(array_agg(row)) from ( " + request.getDistributedQuery()
				+ ") row";
		
		String result = datasource.executeQuery(query);
		result = result.replace("'", "__");
		result = "from (select json_array_elements(data) as rowdata from ( select cast('"+ result+"' as json) as data ) t)t";
		String consolidationQuery = request.getConsolidationQuery();
		
		if(consolidationQuery.contains("GROUP BY") && consolidationQuery.contains("PATIENT_ID")){
			return result;
		}
		consolidationQuery = "select cast(array_to_json(array_agg(row)) as text) from ( select "+consolidationQuery.replaceAll("VITREOS_REPLACER_STRING", result)+") row";
		if(consolidationQuery.endsWith("]")){
			consolidationQuery = consolidationQuery.substring(0, consolidationQuery.length()-1);
		}
		String str = (String)entityManager.createNativeQuery(consolidationQuery).getResultList().get(0);
		
		//Clean all variables
		query = null;
		result = null;
		consolidationQuery = null;

		return str;
	}
}
