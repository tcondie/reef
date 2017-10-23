package org.apache.reef.runtime.yarn.client;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TestLocalYarnClientConfiguration {

    private final static Logger LOG = Logger.getLogger(TestLocalYarnClientConfiguration.class.getName());

    @Test
    public void testLocalYarnClientConfiguration() throws Exception {
        final Configuration RUNTIME_CONF = LocalYarnClientConfiguration.CONF
                .set(LocalYarnClientConfiguration.ROOT_FOLDER, "C:\\Users\\tcondie\\")
                .build();

        final Configuration DRIVER_CONF = DriverConfiguration.CONF
                .set(DriverConfiguration.DRIVER_IDENTIFIER, "test")
                .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TestDriver.class))
                .set(DriverConfiguration.ON_DRIVER_STARTED, TestDriver.StartHandler.class)
                .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, TestDriver.EvaluatorAllocatedHandler.class)
                .set(DriverConfiguration.DRIVER_JOB_SUBMISSION_DIRECTORY, "/tmp")
                .build();
        final LauncherStatus status = DriverLauncher.getLauncher(RUNTIME_CONF).run(DRIVER_CONF, 0);
        LOG.log(Level.INFO, "Launch status: " + status.toString());
    }



    @Unit
    private final static class TestDriver {

        final class StartHandler implements EventHandler<StartTime> {
            @Override
            public void onNext(StartTime value) {

            }
        }

        final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
            @Override
            public void onNext(AllocatedEvaluator value) {

            }
        }
    }
}
