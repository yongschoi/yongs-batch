package yongs.temp.dao;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import yongs.temp.model.Status;

public interface StatusRepository extends MongoRepository<Status, String> {
	public List<Status> findByGrade(final String grade);
}