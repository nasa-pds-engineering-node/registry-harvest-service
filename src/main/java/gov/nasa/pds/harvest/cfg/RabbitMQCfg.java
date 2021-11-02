package gov.nasa.pds.harvest.cfg;

import java.util.ArrayList;
import java.util.List;

/**
 * RabbitMQ configuration
 * @author karpenko
 */
public class RabbitMQCfg
{
    /**
     * List of RabbitMQ addresses (host and port tuples)
     */
    public List<IPAddress> addresses = new ArrayList<>();
    
    /**
     * RabbitMQ user
     */
    public String userName;
    
    /**
     * RabbitMQ password
     */
    public String password;

}
