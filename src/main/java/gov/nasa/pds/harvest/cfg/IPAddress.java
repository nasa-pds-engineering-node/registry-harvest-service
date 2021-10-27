package gov.nasa.pds.harvest.cfg;

/**
 * RabbitMQ address. Host and port tuple.
 * @author karpenko
 */
public class IPAddress
{
    private String host;
    private int port;

    /**
     * Constructor
     * @param host a host, such as "localhost"
     * @param port a port, such as 5672
     */
    public IPAddress(String host, int port)
    {
        this.host = host;
        this.port = port;
    }
        
    public String getHost()
    {
        return host;
    }
    
    public int getPort()
    {
        return port;
    }
    
    @Override
    public String toString()
    {
        return host + ":" + port;
    }
}
