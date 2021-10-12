package gov.nasa.pds.harvest.proc;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.harvest.util.xml.PdsLabelInfo;
import gov.nasa.pds.harvest.util.xml.PdsLabelInfoParser;


public class DirectoryProcessor
{
    private int batchSize = 20;
    private PdsLabelInfoParser labelInfoParser = new PdsLabelInfoParser();
    
    public DirectoryProcessor()
    {
    }


    public void process(File dir) throws Exception
    {
        DirectoryStream.Filter<Path> labelFilter = new DirectoryStream.Filter<Path>() 
        {
            public boolean accept(Path path) throws IOException 
            {
                String fileName = path.getFileName().toString().toLowerCase();
                return (fileName.endsWith(".xml"));
            }
        };
        
        DirectoryStream<Path> labelStream = Files.newDirectoryStream(dir.toPath(), labelFilter);
        try
        {
            Map<String, String> batch = new TreeMap<>();

            Iterator<Path> itr = labelStream.iterator();
            while(itr.hasNext())
            {
                File file = itr.next().toFile();
                PdsLabelInfo info = labelInfoParser.getBasicInfo(file);
                batch.put(info.lidvid, file.getAbsolutePath());
                
                if(batch.size() >= batchSize)
                {
                    processBatch(batch);
                    batch = new TreeMap<>();
                }
            }
            
            processBatch(batch);
        }
        finally
        {
            CloseUtils.close(labelStream);
        }
    }

    
    private void processBatch(Map<String, String> batch) throws Exception
    {
        if(batch.size() == 0) return;
        
        batch.keySet().forEach((id) -> { System.out.println(id); });
        System.out.println();
    }
}
