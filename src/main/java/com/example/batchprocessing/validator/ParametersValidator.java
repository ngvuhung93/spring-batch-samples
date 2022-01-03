package com.example.batchprocessing.validator;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.stereotype.Component;

@Component
public class ParametersValidator implements JobParametersValidator {
    @Override
    public void validate(JobParameters jobParameters) throws JobParametersInvalidException {
        String filePathParam = jobParameters.getString("filePath");
        if(filePathParam == null || filePathParam.isBlank()){
            throw new JobParametersInvalidException("filePath is mandatory parameter and should not be blank");
        }
    }
}
