package gov.nasa.pds.harvest.cfg;

public class IPAddress
{
    private String host;
    private int port;

    public IPAddress(String host, int port)
    {
        this.host = host;
        this.port = port;
    }
        
    public String getHost()
    {
        return host;
    }
    
    public int getport()
    {
        return port;
    }
    
    @Override
    public String toString()
    {
        return host + ":" + port;
    }
}
