package gov.nasa.pds.harvest.mq;

/**
 * Message queue interface
 * @author karpenko
 */
public interface MQClient
{
    /**
     * Run a message queue client. Usually this method blocks.
     * @throws Exception
     */
    public void run() throws Exception;
    
    /**
     * Get message queue type (e.g., RabbitMQ or ActiveMQ)
     * @return queue type
     */
    public String getType();
    
    /**
     * Get message queue connection info (host:port or URL)
     * @return connection info
     */
    public String getConnectionInfo();
    
    /**
     * Get connection status 
     * @return connection status
     */
    public boolean isConnected();
}
