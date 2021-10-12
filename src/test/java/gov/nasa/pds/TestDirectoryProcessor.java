package gov.nasa.pds;

import java.io.File;

import gov.nasa.pds.harvest.proc.DirectoryProcessor;

public class TestDirectoryProcessor
{

    public static void main(String[] args) throws Exception
    {
        DirectoryProcessor proc = new DirectoryProcessor();
        File dir = new File("/ws3/OREX/orex_spice/spice_kernels/mk");
        proc.process(dir);
    }

}
