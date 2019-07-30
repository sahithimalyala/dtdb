package com.dtdb.master.dispatcher;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Component;

/**
 * @author Surya Pramodh
 * Repository class to perform CRUD operations on {@link PlannerCache} entity
 */
@Component
public interface PlannerCacheRepository extends
		PagingAndSortingRepository<PlannerCache, Integer> {	
	PlannerCache findByActualQuery(String query);
}
