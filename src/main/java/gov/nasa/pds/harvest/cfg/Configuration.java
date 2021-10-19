package gov.nasa.pds.harvest.cfg;

import java.util.ArrayList;
import java.util.List;


public class Configuration
{
    public List<IPAddress> mqAddresses = new ArrayList<>();
    public int webPort;
    
    public RegistryCfg registryCfg = new RegistryCfg();
}
