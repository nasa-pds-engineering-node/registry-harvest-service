package gov.nasa.pds;

import java.io.File;
import java.util.List;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.job.Job;
import gov.nasa.pds.harvest.proc.ProductProcessor;
import gov.nasa.pds.harvest.util.out.RegistryDocWriter;

public class TestProductProcessor
{

    public static void main(String[] args) throws Exception
    {
        Job job = new Job();
        job.jobId = "job123";
        job.nodeName = "TestNode";
        
        File file = new File("/tmp/d5/orex.xml");
        
        HarvestCfg cfg = new HarvestCfg();
        cfg.processDataFiles = false;
        
        RegistryDocWriter writer = new RegistryDocWriter();
        ProductProcessor proc = new ProductProcessor(cfg, writer);
        
        writer.clearData();
        
        proc.processFile(file, job);
        
        List<String> data = writer.getData();
        int size = 0;
        for(String str: data)
        {
            size += str.length();
            System.out.println(str);
        }
        
        System.out.println("size = " + size);
    }
    
}
