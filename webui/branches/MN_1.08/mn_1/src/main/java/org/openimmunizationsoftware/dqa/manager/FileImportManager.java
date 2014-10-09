package org.openimmunizationsoftware.dqa.manager;

import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.wicket.util.file.File;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;

public class FileImportManager extends ManagerThreadMulti
{
  private static FileImportManager singleton = null;

  public static synchronized FileImportManager getFileImportManager()
  {
    if (singleton == null)
    {
      singleton = new FileImportManager();
      singleton.start();
    }
    return singleton;
  }

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
  private File processingFile = null;

  private FileImportManager() {
    super("File Import Manager");
  }

  private static String DQA_IN_FILE_DIR = "dqa.in.file.dir";

  @Override
  public void run()
  {
    internalLog.append("Starting file import manager, loading settings\r");
    while (keepRunning)
    {
      internalLog.append("Looking to import at " + sdf.format(new Date()) + "\r");
      try
      {
        KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
        if (ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_ENABLE, false))
        {
          internalLog.append("Import enabled\r");
          internalLog.append("Looking in system settings first for path\r");
          String rootDirString = System.getProperty(DQA_IN_FILE_DIR);
          if (rootDirString == null || rootDirString.equals(""))
          {
            internalLog.append("Not found in system settings, looking in keyed settings\r");
            rootDirString = ksm.getKeyedValue(KeyedSetting.IN_FILE_DIR, "c:/data/in");
          }
          File rootDir = new File(rootDirString);
          if (rootDir.exists() && rootDir.isDirectory())
          {
            internalLog.append("Root dir " + rootDirString + " exists, begin processing\r");
            processDataDir(ksm, rootDir);
          } else
          {
            internalLog.append("Can't find root directory: " + rootDirString + "\r");
          }
        }
      } catch (Throwable e)
      {
        e.printStackTrace();
        lastException = e;
      }
      internalLog.append("Processing complete\r");
      try
      {
        synchronized (sdf)
        {
          KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
          sdf.wait(ksm.getKeyedValueInt(KeyedSetting.IN_FILE_WAIT, 15) * 1000);
        }
      } catch (InterruptedException ie)
      {
        keepRunning = false;
        return;
      }
      internalLog.setLength(0);
    }
  }

  private void processDataDir(KeyedSettingManager ksm, File rootDir) throws IOException, FileNotFoundException
  {
    internalLog.append("Looking in " + rootDir.getAbsolutePath() + "\r");
    String[] dirNames = rootDir.list(new FilenameFilter() {
      public boolean accept(java.io.File arg0, String arg1)
      {
        return new File(arg0, arg1).isDirectory();
      }
    });
    internalLog.append("Found " + dirNames.length + " directories to look in \r");
    for (String dirName : dirNames)
    {
      if (threads.size() > ksm.getKeyedValueInt(KeyedSetting.IN_FILE_THREAD_COUNT_MAX, 5))
      {
        internalLog.append("Too many threads are currently running, waiting before starting more threads \r");
        break;
      }
      File dir = new File(rootDir, dirName);
      internalLog.append(" + " + dirName);
      ManagerThread thread = threads.get(dir.getName());
      if (thread != null)
      {
        internalLog.append(" PROCESSING \r");
      } else if (!dir.exists())
      {
        internalLog.append(" DIR NOT FOUND \r");
      } else if (!checkForSubmissions(ksm, dir))
      {
        internalLog.append(" NO SUBMISSIONS \r");
      } else if (!createProcessingLogFile(dir))
      {
        internalLog.append(" WAITING \r");
      } else
      {
        internalLog.append(" STARTING \r");
        FileImportProcessor fip = new FileImportProcessor(dir, this);
        threads.put(dir.getName(), fip);
        fip.start();
      }
    }
  }

  private boolean createProcessingLogFile(File dir) throws IOException
  {
    processingFile = new File(dir, "processing.txt");
    boolean okayToProcess = true;
    if (processingFile.exists())
    {
      long age = System.currentTimeMillis() - processingFile.lastModified();
      if (age < 60 * 60 * 1000)
      {
        // this folder might be being processed by another instance of this
        // application
        // this should not happen, but if it is then this process will give it
        // at least
        // an hour to complete. When the processing is complete the file is
        // renamed to
        // processed.txt
        okayToProcess = false;
      }
    }
    return okayToProcess;
  }

  private boolean checkForSubmissions(KeyedSettingManager ksm, File dir)
  {
    File submitDir = new File(dir, ksm.getKeyedValue(KeyedSetting.IN_FILE_SUBMIT_DIR_NAME, "submit"));
    if (!submitDir.exists())
    {
      internalLog.append("Submit directory does not exist, creating \r");
      submitDir.mkdir();
    }
    String[] filesToProcess = submitDir.list(new FilenameFilter() {
      public boolean accept(java.io.File dir, String name)
      {
        File file = new File(dir, name);
        return file.isFile() && file.canRead();
      }
    });
    return filesToProcess.length > 0;
  }
}
