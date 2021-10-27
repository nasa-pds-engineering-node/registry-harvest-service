package gov.nasa.pds.harvest.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.jar.Attributes;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import gov.nasa.pds.harvest.util.ManifestUtils;


/**
 * A servlet to report status of the Crawler server
 * @author karpenko
 */
@SuppressWarnings("serial")
public class StatusServlet extends HttpServlet 
{
    private Gson gson;
    
    private String version;
    private String buildTime;
    
    /**
     * Model class for the status message 
     */
    @SuppressWarnings("unused")
    private static class Status
    {        
        public String application = "Harvest";
        public String version;
        public String buildTime;
        
        public String maxMemory;
        public String totalMemory;
        public String freeMemory;
    }

    
    /**
     * Constructor
     */
    public StatusServlet()
    {
        gson = new GsonBuilder().setPrettyPrinting().create();
        setVersioninfo();
    }
    

    /**
     * Cache version info
     */
    private void setVersioninfo()
    {
        version = this.getClass().getPackage().getImplementationVersion();
        Attributes attrs = ManifestUtils.getAttributes();
        if(attrs != null)
        {
            buildTime = attrs.getValue("Build-Time");
        }
    }

    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        Status status = new Status();
        status.version = version;
        status.buildTime = buildTime;
        status.maxMemory = (Runtime.getRuntime().maxMemory() / 1000000) + " MB";
        status.totalMemory = (Runtime.getRuntime().totalMemory() / 1000000) + " MB";
        status.freeMemory = (Runtime.getRuntime().freeMemory() / 1000000) + " MB";

        String jsonStr = gson.toJson(status);

        resp.setContentType("application/json");
        PrintWriter writer = resp.getWriter();
        writer.print(jsonStr);
    }

}
