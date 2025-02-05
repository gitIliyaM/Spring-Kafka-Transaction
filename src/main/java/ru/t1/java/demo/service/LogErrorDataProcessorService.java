package ru.t1.java.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.t1.java.demo.model.DataSourceErrorLog;
import ru.t1.java.demo.repository.DataSourceErrorLogRepository;
import org.aspectj.lang.JoinPoint;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.slf4j.Logger;

@Service
public class LogErrorDataProcessorService {

    private final DataSourceErrorLogRepository dataSourceErrorLogRepository;
    private static final Logger loggers = LoggerFactory.getLogger(LogErrorDataProcessorService.class);

    @Autowired
    LogErrorDataProcessorService(DataSourceErrorLogRepository dataSourceErrorLogRepository) {
        this.dataSourceErrorLogRepository = dataSourceErrorLogRepository;
    }

    public void logAfterThrowing(JoinPoint joinPoint, Throwable ex)  {
        DataSourceErrorLog errorLog = new DataSourceErrorLog();
        errorLog.setExceptionStackTrace(Arrays.toString(ex.getStackTrace()));
        errorLog.setMessage(ex.getMessage());
        errorLog.setSignatureMethod(joinPoint.getSignature().toShortString());

        try{
            ObjectMapper objectMapper = new ObjectMapper();
            String string = objectMapper.writeValueAsString(errorLog);
            DataSourceErrorLog logList = objectMapper.readValue(string, DataSourceErrorLog.class);
            dataSourceErrorLogRepository.saveAndFlush(logList);
        } catch (Exception exception){
            exceptionText(exception.getMessage().toUpperCase());
        }
    }
    public void exceptionText(String exception){
        loggers.info("====== Exception ======");
        loggers.info("Exception : {}", exception);
        loggers.info("======    END    ======");
    }
}
