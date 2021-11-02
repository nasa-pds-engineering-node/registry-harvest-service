package gov.nasa.pds.harvest.cfg;


/**
 * Harvest server configuration
 * @author karpenko
 */
public class Configuration
{
    /**
     * Message server type
     */
    public MQType mqType;
    
    /**
     * ActiveMQ configuration
     */
    public ActiveMQCfg amqCfg = new ActiveMQCfg();
    
    /**
     * RabbitMQ configuration
     */
    public RabbitMQCfg rmqCfg = new RabbitMQCfg();

    /**
     * Embedded web server port
     */
    public int webPort;

    /**
     * Registry (elasticsearch) configuration
     */
    public RegistryCfg registryCfg = new RegistryCfg();
    
    /**
     * Store original XML PDS labels as BLOBs
     */
    public boolean storeLabels = true;
    
    /**
     * Store JSON formatted PDS labels as BLOBs
     */
    public boolean storeJsonLabels = true;
    
    /**
     * Process data files (referenced in PDS label's File section)
     */
    public boolean processDataFiles = true;
    
}
