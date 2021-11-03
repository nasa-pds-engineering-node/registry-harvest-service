package gov.nasa.pds.harvest.cfg;

public class HarvestCfg
{
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
