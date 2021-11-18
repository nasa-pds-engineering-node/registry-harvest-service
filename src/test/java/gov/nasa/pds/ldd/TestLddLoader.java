package gov.nasa.pds.ldd;

import java.io.File;
import java.time.Instant;
import java.util.Date;

import gov.nasa.pds.harvest.dao.LddInfo;
import gov.nasa.pds.harvest.dd.LddLoader;
import gov.nasa.pds.harvest.util.Log4jConfigurator;


public class TestLddLoader
{

    public static void main(String[] args) throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");

        File lddFile = new File("/tmp/PDS4_PDS_JSON_1500.JSON");
        
        LddInfo lddInfo = new LddInfo();
        Instant instant = lddInfo.lastDate;        
        //Instant instant = new Date().toInstant();
        
        LddLoader ldd = new LddLoader("http://localhost:9200", "t1", null);
        ldd.load(lddFile, "PDS4_PDS_JSON_1500.JSON", "pds", instant);
    }

}
