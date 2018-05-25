package io.pravega.example.iiotdemo.flinkprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {
    private static Logger log = LoggerFactory.getLogger(ApplicationMain.class);

    public static void main(String... args) throws Exception {
        AppConfiguration appConfiguration = new AppConfiguration(args);
        String runMode = appConfiguration.getJobClass();
        Class<?> jobClass = Class.forName(runMode);
        AbstractJob job = (AbstractJob) jobClass.getConstructor(AppConfiguration.class).newInstance(appConfiguration);
        job.run();
    }
}
