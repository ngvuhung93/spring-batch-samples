package com.example.batchprocessing.controller;

import com.example.batchprocessing.service.JobStarterService;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class JobController {

    @Autowired
    private JobStarterService jobStarterService;

    @PostMapping("triggerJob")
    public void triggerJob(@RequestParam String filePath, @RequestParam Boolean async) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        jobStarterService.triggerJob(filePath, async);
    }

}

