package net.pedegie.stats.api.queue.fileaccess;

import net.pedegie.stats.api.queue.LogFileConfiguration;

import java.util.Objects;

class LogFileConfigurationValidator
{
    private static final String EXCEPTION_HEADER = "Wrong configuration of " + LogFileConfigurationValidator.class.getName() + "\n";

    public static void validate(LogFileConfiguration logFileConfiguration)
    {
        Objects.requireNonNull(logFileConfiguration);
        Objects.requireNonNull(logFileConfiguration.getPath());

        if (logFileConfiguration.isAppend())
        {
            if (logFileConfiguration.isNewFileWithDate())
            {
                throwException("append", "newFileWithDate");
            }

            if (logFileConfiguration.isOverride())
            {
                throwException("append", "override");
            }
        }

        if (logFileConfiguration.isNewFileWithDate())
        {
            if (logFileConfiguration.isAppend())
            {
                throwException("newFileWithDate", "append");
            }

            if (logFileConfiguration.isOverride())
            {
                throwException("newFileWithDate", "override");
            }
        }

        if (logFileConfiguration.isOverride())
        {
            if (logFileConfiguration.isAppend())
            {
                throwException("override", "append");
            }

            if (logFileConfiguration.isNewFileWithDate())
            {
                throwException("override", "newFileWithDate");
            }
        }
    }

    private static void throwException(String firstProperty, String secondProperty)
    {
        throw new IllegalArgumentException(EXCEPTION_HEADER +
                "'" + firstProperty + "' property cannot be mixed with '" + secondProperty + "', pick one");
    }
}
