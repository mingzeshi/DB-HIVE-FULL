package org.frozen.test;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestWindows {

    public static void main(String[] args) {
        args = new String[] {
                "-DtableName=bcpdata1_201701_01",
                "-DstartTimeD=1441234567890",
                "-DendTime=1441234567890",
                "-Ddataset=RWA_SOURCE_XXXX,RWA_SOURCE_XXXX",
                "-Dexcludedataset=RWA_SOURCE_XXXX,RWA_SOURCE_XXXX",
                "-Dnamespace=S003"
        };


        for(String prop : args) {
            String[] keyval = prop.split("=", 3);
            if (keyval.length == 2) {
//                conf.set(keyval[0], keyval[1], "from command line");
                System.out.println(keyval[0] + "-" + keyval[1]);
            }
        }

        /*
        TestWindows testWindows = new TestWindows();

        String[] resultArray = testWindows.preProcessForWindows(args);
        for(String result : resultArray) {
            System.out.println(result);
        }
        */
    }

    private String[] preProcessForWindows(String[] args) {
        if (!Shell.WINDOWS) {
            return args;
        }
        if (args == null) {
            return null;
        }
        List<String> newArgs = new ArrayList<String>(args.length);
        for (int i=0; i < args.length; i++) {
            String prop = null;
            if (args[i].equals("-D")) {
                newArgs.add(args[i]);
                if (i < args.length - 1) {
                    prop = args[++i];
                }
            } else if (args[i].startsWith("-D")) {
                prop = args[i];
            } else {
                newArgs.add(args[i]);
            }
            if (prop != null) {
                if (prop.contains("=")) {
                    // everything good
                } else {
                    if (i < args.length - 1) {
                        prop += "=" + args[++i];
                    }
                }
                newArgs.add(prop);
            }
        }

        return newArgs.toArray(new String[newArgs.size()]);
    }
}
