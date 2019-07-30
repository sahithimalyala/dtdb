package com.dtdb.worker.rest;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dtdb.worker.datasource.IVitreosDataSource;
import com.dtdb.worker.datasource.data.ChamberDetailsRepository;

@RestController
public class RefreshChamberService {

	private @Autowired IVitreosDataSource datasource;
	private @Autowired ChamberDetailsRepository chamberDetailsRepository;

	@RequestMapping(value = "/refresh", produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE, method=RequestMethod.POST)
	public Boolean refreshChambers(@RequestBody RefreshDetails details) {
		datasource.closeAll();
		chamberDetailsRepository.deleteAll();
		List<ChamberDetails> shardDetails = details.getChambers();
		for(ChamberDetails c:shardDetails){
			com.dtdb.worker.datasource.data.ChamberDetails chamber = new com.dtdb.worker.datasource.data.ChamberDetails();
			chamber.setDatabase(c.getDatabase());
			chamber.setIpAddress(c.getIpAddress());
			chamber.setName(c.getName());
			chamber.setPassword(c.getPassword());
			chamber.setPort(c.getPort());
			chamberDetailsRepository.save(chamber);
		}
		return true;
	}
}
