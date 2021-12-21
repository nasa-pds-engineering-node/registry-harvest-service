package gov.nasa.pds.harvest;

import java.util.jar.Attributes;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import gov.nasa.pds.harvest.util.Log4jConfigurator;
import gov.nasa.pds.harvest.util.ManifestUtils;


public class HarvestServerMain
{
    public static void main(String[] args)
    {
        // We don't use "java.util" logger.
        Logger log = Logger.getLogger("");
        log.setLevel(Level.OFF);

        // No command-line parameters. Print help.
        if(args.length == 0)
        {
            printHelp();
            System.exit(0);
        }
        
        // Print version
        if(args.length == 1 && ("-V".equals(args[0]) || "--version".equals(args[0])))
        {
            printVersion();
            System.exit(0);
        }
        
        // Create and start Harvest server
        HarvestServer server = createServer(args);
        int rc = server.run();
        if(rc != 0)
        {
            System.exit(rc);
        }
    }


    private static HarvestServer createServer(String[] args)
    {
        try
        {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmdLine = parser.parse(createOptions(), args);
            
            if(!cmdLine.hasOption("c"))
            {
                System.out.println("[ERROR] Missing required '-c' parameter");
                System.out.println();
                printHelp();
                System.exit(1);
            }
            
            // Init logger
            initLogger(cmdLine);

            // Create Harvest server
            HarvestServer server = new HarvestServer(cmdLine.getOptionValue("c"));
            return server;
        }
        catch(Exception ex)
        {
            System.out.println("[ERROR] " + ex.getMessage());
            System.exit(1);
        }
        
        return null;
    }

    
    /**
     * Initialize Log4j logger
     * @param cmdLine Command line parameters
     */
    private static void initLogger(CommandLine cmdLine)
    {
        String verbosity = cmdLine.getOptionValue("v", "INFO");
        String logFile = cmdLine.getOptionValue("l");

        Log4jConfigurator.configure(verbosity, logFile);
    }

    
    private static Options createOptions()
    {
        Options options = new Options();
        Option.Builder bld;
        
        bld = Option.builder("c").hasArg().argName("file");
        options.addOption(bld.build());
        
        bld = Option.builder("l").hasArg().argName("file");
        options.addOption(bld.build());

        bld = Option.builder("v").hasArg().argName("level");
        options.addOption(bld.build());
        
        return options;
    }

    
    
    /**
     * Print help screen.
     */
    public static void printHelp()
    {
        System.out.println("Usage: harvest-server <options>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  -c <config file>   Start Harvest server");
        System.out.println("  -V, --version      Print Harvest version");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -l <file>    Log file. Default is /tmp/harvest/harvest.log");
        System.out.println("  -v <level>   Logger verbosity: DEBUG / ALL, INFO (default), WARN, ERROR");        
    }

    
    /**
     * Print Harvest version
     */
    public static void printVersion()
    {
        String version = HarvestServerMain.class.getPackage().getImplementationVersion();
        System.out.println("Harvest version: " + version);
        Attributes attrs = ManifestUtils.getAttributes();
        if(attrs != null)
        {
            System.out.println("Build time: " + attrs.getValue("Build-Time"));
        }
    }

}
