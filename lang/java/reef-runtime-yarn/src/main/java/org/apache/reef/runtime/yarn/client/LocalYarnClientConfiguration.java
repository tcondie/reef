package org.apache.reef.runtime.yarn.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.common.UserCredentials;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.client.parameters.RootFolder;
import org.apache.reef.runtime.yarn.client.unmanaged.YarnProxyUser;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.util.logging.LoggingSetup;

/**
 * A ConfigurationModule for the YARN resource manager.
 */
@Public
@ClientSide
public class LocalYarnClientConfiguration extends ConfigurationModuleBuilder {

    static {
        LoggingSetup.setupCommonsLogging();
    }

    public static final OptionalParameter<String> ROOT_FOLDER = new OptionalParameter<>();

    public static final ConfigurationModule CONF = new LocalYarnClientConfiguration()
            .merge(CommonRuntimeConfiguration.CONF)
            // Bind YARN-specific classes
            .bindImplementation(JobSubmissionHandler.class, LocalYarnJobSubmissionHandler.class)
            .bindImplementation(DriverConfigurationProvider.class, YarnDriverConfigurationProviderImpl.class)
            .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
            .bindImplementation(UserCredentials.class, YarnProxyUser.class)
            // Bind the parameters given by the user
            // Bind external constructors. Taken from  YarnExternalConstructors.registerClientConstructors
            .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
            .bindNamedParameter(RootFolder.class, ROOT_FOLDER)
            .build();
}
