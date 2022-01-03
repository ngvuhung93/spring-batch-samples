package com.example.batchprocessing.job;

import com.example.batchprocessing.config.BatchConfiguration;
import com.example.batchprocessing.listener.JobCompletionNotificationListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@SpringBatchTest
public class JobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired private JdbcTemplate jdbcTemplate;

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
