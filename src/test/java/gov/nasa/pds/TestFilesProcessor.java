package gov.nasa.pds;

import java.util.List;

import gov.nasa.pds.harvest.mq.msg.FilesMessage;
import gov.nasa.pds.harvest.proc.FilesProcessor;

public class TestFilesProcessor
{

    public static void main(String[] args) throws Exception
    {
        FilesProcessor proc = new FilesProcessor();
        
        FilesMessage msg = new FilesMessage("123");
        List<String> files = proc.getFilesToProcess(msg);
        System.out.println(files);
    }

}
