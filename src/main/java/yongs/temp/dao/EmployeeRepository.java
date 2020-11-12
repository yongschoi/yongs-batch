package yongs.temp.dao;

import org.springframework.data.mongodb.repository.MongoRepository;

import yongs.temp.model.Employee;

public interface EmployeeRepository extends MongoRepository<Employee, String> {
 
}