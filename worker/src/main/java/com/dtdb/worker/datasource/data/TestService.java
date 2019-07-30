package com.dtdb.worker.datasource.data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dtdb.worker.datasource.IVitreosDataSource;

@RestController
public class TestService {
	
	private @Autowired IVitreosDataSource datasource;
	
	@RequestMapping(value="/test", produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
	public String test(@RequestParam(name="query") String query){
		System.out.println("Test");
		datasource.initialize();
		query = "select array_to_json(array_agg(row)) from ( " +query+" ) row";
		return datasource.executeQuery(query);
	}
	
}
