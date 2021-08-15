package net.pedegie.stats.sb.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Arrays;

public class ProgramArguments
{
    private final int producerThreads;
    private final int consumerThreads;
    private final int messagesToSendPerThread;
    private final double delayMillisToWaitBetweenMessages;
    private final int warmupIterations;

    private ProgramArguments(int producerThreads, int consumerThreads, int messagesToSendPerThread, double millisToWaitBetweenMessages, int warmupIterations)
    {
        this.producerThreads = producerThreads;
        this.consumerThreads = consumerThreads;
        this.messagesToSendPerThread = messagesToSendPerThread;
        this.delayMillisToWaitBetweenMessages = millisToWaitBetweenMessages;
        this.warmupIterations = warmupIterations;
    }

    public static ProgramArguments initialize(String[] commandLineArguments)
    {
        Options options = generateOptions();
        final CommandLineParser cmdLineParser = new DefaultParser();
        CommandLine commandLine = null;
        try
        {
            commandLine = cmdLineParser.parse(options, commandLineArguments);
        }
        catch (ParseException parseException)
        {
            System.out.println(
                    "ERROR: Unable to parse command-line arguments "
                            + Arrays.toString(commandLineArguments) + " due to: "
                            + parseException);
            printHelp(options);
            System.exit(1);
        }

        int producerThreads = Integer.parseInt(commandLine.getOptionValue("p"));
        int consumerThreads = Integer.parseInt(commandLine.getOptionValue("c"));
        int messagesToSendPerThread = Integer.parseInt(commandLine.getOptionValue("m"));
        double delay = Double.parseDouble(commandLine.getOptionValue("d"));
        int warmupIterations = Integer.parseInt(commandLine.getOptionValue("w", "3"));

        return new ProgramArguments(producerThreads, consumerThreads, messagesToSendPerThread, delay, warmupIterations);
    }


    private static Options generateOptions()
    {
        Option producerThreads = Option.builder("p")
                .required(true)
                .hasArg(true)
                .desc("Producer threads.")
                .build();

        Option consumerThreads = Option.builder("c")
                .required(true)
                .hasArg(true)
                .desc("Consumer threads.")
                .build();

        Option messages = Option.builder("m")
                .required(true)
                .hasArg(true)
                .desc("Messages to send per thread.")
                .build();

        Option delay = Option.builder("d")
                .required(true)
                .hasArg(true)
                .desc("Delay in millis to wait between sending messages\n-d 0.001 means delay each message 1 microsecond")
                .build();

        Option warmup = Option.builder("w")
                .required(false)
                .hasArg(true)
                .desc("Warmup iterations")
                .build();

        Options options = new Options();
        options.addOption(producerThreads);
        options.addOption(consumerThreads);
        options.addOption(messages);
        options.addOption(delay);
        options.addOption(warmup);
        return options;
    }

    private static void printHelp(final Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        String syntax = "StatsBenchmark";
        System.out.println("\n====");
        System.out.println("HELP");
        System.out.println("====");
        formatter.printHelp(syntax, options);
    }

    public int getProducerThreads()
    {
        return producerThreads;
    }

    public int getConsumerThreads()
    {
        return consumerThreads;
    }

    public int getMessagesToSendPerThread()
    {
        return messagesToSendPerThread;
    }

    public double getDelayMillisToWaitBetweenMessages()
    {
        return delayMillisToWaitBetweenMessages;
    }

    public int getWarmupIterations()
    {
        return warmupIterations;
    }
}
