package com.dtdb.master.dispatcher;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Component;

/**
 * @author Surya Pramodh
 * Repository class to perform CRUD operations on {@link WorkerDetails} entity
 */
@Component
public interface WorkerDetailsRepository extends
		PagingAndSortingRepository<WorkerDetails, Integer> {	
}
