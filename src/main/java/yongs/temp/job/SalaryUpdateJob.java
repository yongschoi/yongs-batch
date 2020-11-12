package yongs.temp.job;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import yongs.temp.dao.EmployeeRepository;
import yongs.temp.dao.StatusRepository;
import yongs.temp.model.Employee;
import yongs.temp.model.Status;

@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class})
@EnableBatchProcessing
@Configuration
public class SalaryUpdateJob {
/*
	private Logger logger = LoggerFactory.getLogger(SalaryUpdateBatch.class);
	@Autowired
	private EmployeeRepository employeeRepo;
	@Autowired
	private StatusRepository statusRepo;

	private int chunkSize;
	@Value("${chunkSize:100}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
    
	@Bean
	public Job salaryUpdateJob(JobBuilderFactory jobBuilderFactory, Step salaryUpdateStep) {
		logger.info("***** Salary Update Job *****");
		
		return jobBuilderFactory.get("salaryUpdateJob")
				.preventRestart()
				.start(salaryUpdateStep)
				.build();
	}
	
	@Bean
	@JobScope
	public Step salaryUpdateStep(StepBuilderFactory stepBuilderFactory) {
		logger.info("***** Salary Update Step *****");
		return stepBuilderFactory.get("salaryUpdateStep")
				.<Employee, Employee> chunk(chunkSize)
				.reader(salaryUpdateReader())
				.processor(salaryUpdateProcessor())
				.writer(salaryUpdateWriter())
				.build();
	}
	
	@Bean
	@StepScope
	public ListItemReader<Employee> salaryUpdateReader() {
		logger.info("***** <Salary Update Reader> *****");
		List<Status> statusList = statusRepo.findByGrade("A");
		logger.debug("***** Number of A grade : " + statusList.size());
		List<Employee> employeeList = new ArrayList<Employee>();
		for (Status status : statusList) {
			Employee e = employeeRepo.findById(status.getId()).get();
			employeeList.add(e);
		}
		return new ListItemReader<Employee>(employeeList);	
	}
	@Bean
	@StepScope
	public ItemProcessor<Employee, Employee> salaryUpdateProcessor() {
		logger.info("***** <Salary Update Processor> *****");
		return (employee -> {
			double oldSalary = employee.getSalary();
			double newSalary = oldSalary + oldSalary*0.1;
			employee.setSalary(newSalary);
			return employee;
		});	
	}
	@Bean
	@StepScope
	public ItemWriter<Employee> salaryUpdateWriter() {
		logger.info("***** <Salary Update Writer> *****");
		return (employees -> employeeRepo.saveAll(employees));
	}
*/
	
/*	
	@Bean
	public ItemProcessor<Employee, Employee> gradeAProcessor() {
		logger.info("***** Processor Start *****");
		return new ItemProcessor<Employee, Employee>() {
			@Override
			public Employee process(Employee e) throws Exception {		
				double oldSalary = e.getSalary();
				double newSalary = oldSalary + oldSalary*0.1;
				e.setSalary(newSalary);
				return e;
			}
		};	
	}
	
	@Bean
	public ItemWriter<Employee> gradeAWriter() {
		logger.info("***** Writer Start *****");
		return new ItemWriter<Employee>() {
			@Override
			public void write(List<? extends Employee> employees) throws Exception {
				employeeRepo.saveAll(employees);				
			}
		};
	}
*/
}
