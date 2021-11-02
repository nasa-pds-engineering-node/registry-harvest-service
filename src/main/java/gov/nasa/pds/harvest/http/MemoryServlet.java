package gov.nasa.pds.harvest.http;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * A servlet to report status of the Crawler server
 * @author karpenko
 */
@SuppressWarnings("serial")
public class MemoryServlet extends HttpServlet 
{
    private Gson gson;
    
    /**
     * Model class for the status message 
     */
    @SuppressWarnings("unused")
    private static class Info
    {        
        public String maxMemory;
        public String totalMemory;
        public String freeMemory;
        public String usedMemory;
    }

    
    /**
     * Constructor
     */
    public MemoryServlet()
    {
        gson = new GsonBuilder().setPrettyPrinting().create();
    }
    

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        Info info = new Info();
        
        int maxMem = (int)(Runtime.getRuntime().maxMemory() / 1_000_000);
        int totalMem = (int)(Runtime.getRuntime().totalMemory() / 1_000_000);
        int freeMem = (int)(Runtime.getRuntime().freeMemory() / 1_000_000);
        
        info.maxMemory = maxMem + " MB";
        info.totalMemory = totalMem + " MB";
        info.freeMemory = freeMem + " MB";
        info.usedMemory = (totalMem - freeMem) + " MB";

        String jsonStr = gson.toJson(info);

        resp.setContentType("application/json");
        PrintWriter writer = resp.getWriter();
        writer.print(jsonStr);
    }

}
