package gov.nasa.pds;

import java.util.List;

import gov.nasa.pds.harvest.dao.DataLoader;


public class TestDataLoader
{

    public static void main(String[] args) throws Exception
    {
        DataLoader ldr = new DataLoader("http://localhost:9200", "test", null);
        
        List<String> data = null;
        ldr.loadBatch(data);
    }

}
