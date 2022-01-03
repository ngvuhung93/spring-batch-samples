package com.example.batchprocessing.job;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JobTest {

    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired private JobLauncher jobLauncher;
    @Autowired private JobRepository jobRepository;

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private DataSource dataSource;

    @Autowired
    private Job importUserJob;

    @Before
    public void before(){
        jdbcTemplate = new JdbcTemplate(dataSource);
        jobLauncherTestUtils = new JobLauncherTestUtils();
        this.jobLauncherTestUtils.setJobLauncher(jobLauncher);
        this.jobLauncherTestUtils.setJobRepository(jobRepository);
        this.jobLauncherTestUtils.setJob(importUserJob);
    }

    @Test
    public void givenJobWithValidData_whenImportUserJob() throws Exception {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addString("filePath", "sample-data.csv");

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(builder.toJobParameters());
        assertNotNull(jobExecution);

        List userList = jdbcTemplate.queryForList("SELECT * FROM people");
        System.out.println(userList);
        assertEquals(5, userList.size());

        assertEquals(BatchStatus.COMPLETED,jobExecution.getStatus());
        assertNotNull(jobExecution.getStepExecutions());
        assertFalse(jobExecution.getStepExecutions().isEmpty());

        StepExecution stepExecution = jobExecution.getStepExecutions().iterator().next();
        assertEquals(BatchStatus.COMPLETED , stepExecution.getStatus());
        assertEquals(5 , stepExecution.getReadCount());
        assertEquals(0 , stepExecution.getReadSkipCount());
        assertEquals(5 , stepExecution.getWriteCount());
        assertEquals(0 , stepExecution.getWriteSkipCount());
    }

    @Test
    public void givenJobWithBadData_whenImportUserJob() throws Exception {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addString("filePath", "sample-data-baddata.csv");

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(builder.toJobParameters());
        assertNotNull(jobExecution);

        assertEquals(BatchStatus.FAILED,jobExecution.getStatus());
    }

    @Test
    public void givenJobWithNoFilePathParam_whenImportUserJob() throws Exception {
        JobParametersBuilder builder = new JobParametersBuilder();
        assertThrows(JobParametersInvalidException.class,
                () -> jobLauncherTestUtils.launchJob(builder.toJobParameters()));
    }

}
