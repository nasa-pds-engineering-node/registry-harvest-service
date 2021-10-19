package gov.nasa.pds.harvest.util;

public class ThreadUtils
{
    public static void sleepSec(int sec)
    {
        try
        {
            Thread.sleep(sec * 1000);
        }
        catch(InterruptedException ex)
        {
            // Ignore
        }
    }

}
