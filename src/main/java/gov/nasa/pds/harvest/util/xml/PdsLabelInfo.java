package gov.nasa.pds.harvest.util.xml;

public class PdsLabelInfo
{
    public String productClass;
    public String lidvid;
    public String lid;
    public String vid;
    
    
    @Override
    public String toString()
    {
        return String.format("[%s, %s]", productClass, lidvid);
    }
}
