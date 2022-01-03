package com.example.batchprocessing.service;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class JobStarterService {

    private final SimpleJobLauncher syncJobLauncher;
    private final SimpleJobLauncher asyncJobLauncher;
    private final Job importUserJob;

    public JobStarterService(TaskExecutor taskExecutor, JobRepository jobRepository, Job importUserJob){
        syncJobLauncher = jobLauncher(new SyncTaskExecutor(), jobRepository);
        asyncJobLauncher = jobLauncher(new SimpleAsyncTaskExecutor(), jobRepository);
        this.importUserJob = importUserJob;
    }

    public SimpleJobLauncher jobLauncher(TaskExecutor taskExecutor, JobRepository jobRepository){
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        return jobLauncher;
    }

    public JobExecution triggerJob(String filePath, boolean isAsyncJob) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, JobParameter> parameterMap = new HashMap<>();
        parameterMap.put("filePath", new JobParameter(filePath));
        JobParameters jobParameters = new JobParameters(parameterMap);
        JobLauncher jobLauncher = isAsyncJob ? asyncJobLauncher : syncJobLauncher;
        return jobLauncher.run(importUserJob, jobParameters);
    }

}
