package yongs.temp.job;

import java.util.HashMap;
import java.util.Map;

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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import yongs.temp.dao.EmployeeRepository;
import yongs.temp.model.Employee;
import yongs.temp.model.Status;

@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class})
@EnableBatchProcessing
@Configuration
public class MultiThreadSalaryUpdateJob {
/*
	private Logger logger = LoggerFactory.getLogger(MultiThreadSalaryUpdateJob.class);
	@Autowired
	private EmployeeRepository employeeRepo;
	
	@Autowired
	private MongoOperations mongoOperation;
	
	private int chunkSize;
	@Value("${chunkSize:1000}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
	
	private int poolSize;
    @Value("${poolSize:10}")
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
    
    @Bean
    public TaskExecutor executor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }
    
	@Bean
	public Job multiThreadSalaryUpdateJob(JobBuilderFactory jobBuilderFactory, Step salaryUpdateStep) {
		logger.info("***** (MultiThread) Salary Update Job *****");	
		return jobBuilderFactory.get("multiThreadSalaryUpdateJob")
				.preventRestart()
				.start(salaryUpdateStep)
				.build();
	}
	
	@Bean
	@JobScope
	public Step multiThreadSalaryUpdateStep(StepBuilderFactory stepBuilderFactory) throws Exception {
		logger.info("***** (MultiThread) Salary Update Step *****");
		return stepBuilderFactory.get("multiThreadSalaryUpdateStep")
				.<Status, Employee> chunk(chunkSize)
				.reader(multiThreadSalaryUpdateReader())
				.processor(multiThreadSalaryUpdateProcessor())
				.writer(multiThreadSalaryUpdateWriter())
				.taskExecutor(executor())
				.throttleLimit(poolSize)
				.build();
	}

	@Bean
	@StepScope
	public ItemReader<Status> multiThreadSalaryUpdateReader() throws Exception {
		logger.info("***** <Salary Update Reader> *****");
		MongoItemReader<Status> statusItemReader = new MongoItemReader<Status>();
		statusItemReader.setTemplate(mongoOperation);
		statusItemReader.setTargetType(Status.class);

		statusItemReader.setQuery("{grade:'A'}");
		Map<String, Direction> sorts = new HashMap<String, Sort.Direction>(1);
		sorts.put("_id", Direction.ASC);
		statusItemReader.setSort(sorts);
		// chunkSize만큼 paging으로 읽어온다.
		statusItemReader.setPageSize(chunkSize);
		
		return statusItemReader;
	}
	@Bean
	@StepScope
	public ItemProcessor<Status, Employee> multiThreadSalaryUpdateProcessor() {
		logger.info("***** <Salary Update Processor> *****");		
		return status -> {
			Employee e = employeeRepo.findById(status.getId()).get();
			double oldSalary = e.getSalary();
			double newSalary = oldSalary + oldSalary*0.1;
			e.setSalary(newSalary);
			return e;
		};	
	}
	@Bean
	@StepScope
	public ItemWriter<Employee> multiThreadSalaryUpdateWriter() {
		logger.info("***** <Salary Update Writer> *****");		
		return employees -> employeeRepo.saveAll(employees);
	}
*/
}
