package gov.nasa.pds.harvest.mq.msg;

import java.util.List;

public class FilesMessage
{
    public String jobId;
    public List<String> files;
    public List<String> ids;
    
    
    public FilesMessage(String jobId)
    {
        this.jobId = jobId;
    }
}
