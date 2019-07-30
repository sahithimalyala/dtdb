package com.dtdb.worker.datasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Component;

import com.dtdb.worker.datasource.data.ChamberDetails;
import com.dtdb.worker.datasource.data.ChamberDetailsRepository;
import com.jolbox.bonecp.BoneCPDataSource;

@Component
public class VitreosDataSource implements IVitreosDataSource {
	private static final Logger logger = org.slf4j.LoggerFactory
			.getLogger(VitreosDataSource.class);

	/**
	 * To hold all the data sources configuration in the database.
	 */
	private List<DataSource> datasources = new ArrayList<DataSource>();
	private @Autowired ChamberDetailsRepository repository;
	private Boolean _isInitialized = false;

	@Override
	public void initialize() {
		if(_isInitialized) return;
		logger.debug("Initializing Vitreos Datasources");
		Iterator<ChamberDetails> storageIterator = repository.findAll()
				.iterator();
		while (storageIterator.hasNext()) {
			ChamberDetails chamber = storageIterator.next();
			logger.debug("Building DataSources for chamber: "
					+ chamber.getName());
			BoneCPDataSource dataSource = new BoneCPDataSource();
			 dataSource.setDriverClass("org.postgresql.Driver");
		        dataSource.setJdbcUrl("jdbc:postgresql://" + chamber.getIpAddress() + ":"
						+ chamber.getPort() + "/" + chamber.getDatabase());
		        dataSource.setUsername(chamber.getUser());
		        dataSource.setPassword(chamber.getPassword());
		        dataSource.setIdleMaxAgeInMinutes(5);
		        dataSource.setMaxConnectionsPerPartition(5);
		        dataSource.setMinConnectionsPerPartition(1);
		        dataSource.setPartitionCount(2);
		        dataSource.setAcquireIncrement(2);
		        dataSource.setStatementsCacheSize(50);
		        dataSource.setIdleConnectionTestPeriodInSeconds(20);
			datasources.add(dataSource);
		}
		logger.debug("Total Datasources build on application load are: "
				+ datasources.size());
		_isInitialized = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.vitreos.worker.datasource.IVitreosDataSource#executeQuery(java.lang
	 * .String)
	 */
	@Override
	public String executeQuery(String query) {
		// Create a fixed pool size
		System.out.println(System.currentTimeMillis());
		ExecutorService service = Executors.newFixedThreadPool(datasources
				.size());
		List<Callable<String>> tasks = new ArrayList<Callable<String>>();
		try {
			for (DataSource ds : datasources) {
				Callable<String> task = () -> {
					Connection connection = null;
					try {
						long time = System.currentTimeMillis();
						connection = ds.getConnection();

						Statement statement = connection.createStatement();
						ResultSet resultSet = statement.executeQuery(query);
						String result = null;
						while (resultSet.next()) {
							result = resultSet.getString(1);
						}
						System.out.println("Total Time for Datasource: "+(System.currentTimeMillis()-time));
						connection.close();
						return result;
					} catch (Exception e) {
						e.printStackTrace();
						throw new Exception("task interrupted", e);
					}finally{
						if(connection!=null){
							connection.close();
						}
					}
				};
				tasks.add(task);
			}
			List<Future<String>> results = service.invokeAll(tasks);
			List<String> resultJSONs = new ArrayList<String>();
			for(Future<String> future: results){
				String resultSet = future.get();
				if(resultSet!=null && resultSet.trim().length()>0){
					resultJSONs.add(resultSet.replace("[", "").replace("]", ""));
				}
			}
			System.out.println(System.currentTimeMillis());
			return resultJSONs.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	@Override
	public void closeAll() {
		for(DataSource datasourse: datasources){
			BoneCPDataSource ds = (BoneCPDataSource)datasourse;
			ds.close();
		}
	}

	@Override
	public void reIniitialize() {
		_isInitialized = false;
		initialize();
	}

}
