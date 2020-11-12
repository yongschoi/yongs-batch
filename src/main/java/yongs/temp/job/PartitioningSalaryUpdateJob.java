package yongs.temp.job;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;

import yongs.temp.dao.EmployeeRepository;
import yongs.temp.model.Employee;
import yongs.temp.model.Status;
import yongs.temp.partitioner.SalaryUpdatePartitioner;

@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class})
@EnableBatchProcessing
@Configuration
public class PartitioningSalaryUpdateJob {
	private Logger logger = LoggerFactory.getLogger(PartitioningSalaryUpdateJob.class);
	@Autowired
    private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private EmployeeRepository employeeRepo;	
	@Autowired
    MongoTemplate mongoTemplate;
	
	private int chunkSize;
	@Value("${chunkSize:1000}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
	
	private int gridSize;
    @Value("${gridSize:10}")
    public void setGridSize(int gridSize) {
        this.gridSize = gridSize;
    }

	@Bean
	public Job patitioningSalaryUpdateJob(JobBuilderFactory jobBuilderFactory) {
		logger.info("***** (Partitioning) Salary Update Job *****");	
		return jobBuilderFactory.get("patitioningSalaryUpdateJob")
				.preventRestart()
				.start(masterStep())
				.build();
	}

	// Master Step
	@Bean
	public Step masterStep() {
		logger.info("***** (Partitioning) Salary Update Master Step *****");
		return stepBuilderFactory.get("masterStep")
				.partitioner(slaveStep().getName(), new SalaryUpdatePartitioner())
				.step(slaveStep())
				.gridSize(gridSize)
				.taskExecutor(new SimpleAsyncTaskExecutor())
				.build();
	}
	// Slave Step
	@Bean
	public Step slaveStep() {
		logger.info("***** (Partitioning) Salary Update Slave Step *****");
		return stepBuilderFactory.get("slaveStep")
				.<Status, Employee> chunk(chunkSize)
				.reader(partitioningSalaryUpdateReader(null))
				.processor(partitioningSalaryUpdateProcessor())
				.writer(partitioningSalaryUpdateWriter())
				.build();
	}
	
	@Bean
	@StepScope
	public ItemReader<Status> partitioningSalaryUpdateReader(
			@Value("#{stepExecutionContext['no']}") Integer no) {
		logger.info("***** <Salary Update Reader> *****");
		
		MongoItemReader<Status> statusItemReader = new MongoItemReader<Status>();	
		statusItemReader.setTemplate(mongoTemplate);
		statusItemReader.setTargetType(Status.class);
		statusItemReader.setQuery("{grade:'A', $where:'parseInt(this._id)%" + gridSize + "==" + no +"'}");
		
		Map<String, Direction> sorts = new HashMap<String, Sort.Direction>(1);
		sorts.put("_id", Direction.ASC);
		statusItemReader.setSort(sorts);
		
		// chunkSize만큼 paging으로 읽어온다.
		statusItemReader.setPageSize(chunkSize);
		
		return statusItemReader;
	}
	@Bean
	@StepScope
	public ItemProcessor<Status, Employee> partitioningSalaryUpdateProcessor() {
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
	public ItemWriter<Employee> partitioningSalaryUpdateWriter() {
		logger.info("***** <Salary Update Writer> *****");		
		return employees -> employeeRepo.saveAll(employees);
	}
}
