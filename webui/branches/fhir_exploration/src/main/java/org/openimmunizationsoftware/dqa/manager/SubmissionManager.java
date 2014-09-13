/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.Submission;

public class SubmissionManager extends ManagerThreadMulti
{
  private static SubmissionManager singleton = null;

  public static synchronized SubmissionManager getSubmissionManager()
  {
    if (singleton == null)
    {
      singleton = new SubmissionManager();
      singleton.start();
    }
    return singleton;
  }

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");

  private SubmissionManager() {
    super("Submission Manager");
  }

  @Override
  public void run()
  {
    internalLog.append("Starting submission manager, loading settings\r");
    while (keepRunning)
    {
      internalLog.append("Looking to import at " + sdf.format(new Date()) + "\r");
      try
      {
        KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
        if (ksm.getKeyedValueBoolean(KeyedSetting.IN_SUBMISSION_ENABLE, false))
        {
          internalLog.append("Import enabled\r");
          processDataDir(ksm);
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

  private void processDataDir(KeyedSettingManager ksm) throws IOException, FileNotFoundException
  {
    internalLog.append("Looking for submitted files \r");
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Query query = session.createQuery("select distinct(submitterName) from Submission where submissionStatus = ?");
    query.setParameter(0, Submission.SUBMISSION_STATUS_SUBMITTED);
    List<String> requestNameList = query.list();

    internalLog.append("Found " + requestNameList.size() + " submissions waiting for processing \r");
    for (String submitterName : requestNameList)
    {
      if (threads.size() > ksm.getKeyedValueInt(KeyedSetting.IN_FILE_THREAD_COUNT_MAX, 5))
      {
        internalLog.append("Too many threads are currently running, waiting before starting more threads \r");
        break;
      }
      internalLog.append(" + " + submitterName);
      ManagerThread thread = threads.get(submitterName);
      if (thread != null)
      {
        internalLog.append(" PROCESSING \r");
      } else
      {
        internalLog.append(" STARTING \r");
        SubmissionProcessor sip = new SubmissionProcessor(submitterName, this);
        threads.put(submitterName, sip);
        sip.start();
      }
    }
    internalLog.append("Currently processing: \r");
    for (String submitterName : threads.keySet())
    {
      internalLog.append(" + " + submitterName + " \r");
    }
  }

}
