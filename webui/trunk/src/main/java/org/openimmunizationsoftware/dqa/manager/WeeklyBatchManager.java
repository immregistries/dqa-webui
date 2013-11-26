/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.wicket.util.file.File;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.BatchActions;
import org.openimmunizationsoftware.dqa.db.model.BatchCodeReceived;
import org.openimmunizationsoftware.dqa.db.model.BatchIssues;
import org.openimmunizationsoftware.dqa.db.model.BatchReport;
import org.openimmunizationsoftware.dqa.db.model.BatchType;
import org.openimmunizationsoftware.dqa.db.model.BatchVaccineCvx;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.MessageHeader;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.ReceiveQueue;
import org.openimmunizationsoftware.dqa.db.model.SubmitStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.quality.QualityCollector;
import org.openimmunizationsoftware.dqa.quality.QualityReport;

public class WeeklyBatchManager extends ManagerThread
{

  private static WeeklyBatchManager singleton = null;

  public static synchronized WeeklyBatchManager getWeeklyBatchManager()
  {
    if (singleton == null)
    {
      singleton = new WeeklyBatchManager();
      singleton.start();
    }
    return singleton;
  }

  private WeeklyBatchManager() {
    super("WeeklyBatchManager");
  }

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");

  private int weekStartDay;
  private Date processingStartTime;
  private Date processingEndTime;

  private Calendar endOfWeek;
  private Calendar startOfWeek;

  @Override
  public void run()
  {
    internalLog.append("Starting weekly batch manager\r");
    while (keepRunning)
    {
      try
      {
        setWeeklyParameters();
        Date now = new Date();
        if (processingStartTime.before(now) && processingEndTime.after(now))
        {
          runNow(now);
        } else
        {
          internalLog.append("Weekly Batch Manager will not run now\r");
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
          sdf.wait(10 * 1000 * 60); // 10 minutes
        }
      } catch (InterruptedException ie)
      {
        keepRunning = false;
        return;
      }
      internalLog.setLength(0);
    }
  }

  @Override
  public void runNow(Date now)
  {
    determineStartAndEndOfWeek(now);
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();
    Query query = session
        .createQuery("from SubmitterProfile where (profile_status = 'Prod' or profile_status = 'Test' or profile_status = 'Hold') and profile_id >= 1200");
    List<SubmitterProfile> profiles = query.list();
    tx.commit();
    for (SubmitterProfile profile : profiles)
    {
      tx = session.beginTransaction();
      internalLog.append("Looking at profile " + profile.getProfileLabel() + " for " + profile.getProfileCode() + "\r");
      query = session.createQuery("from MessageBatch where profile = ? and batchType = ? and endDate = ?");
      query.setParameter(0, profile);
      query.setParameter(1, BatchType.WEEKLY);
      query.setParameter(2, endOfWeek.getTime());
      List<MessageBatch> messageBatchList = query.list();
      tx.commit();
      if (messageBatchList.size() == 0)
      {
        internalLog.append(" + Creating batch\r");
        // batch has not yet been created. Create one now
        // Get all messages received in the week
        createWeeklyBatch(session, profile);
      } else
      {
        internalLog.append(" + No batches to create\r");
      }
    }
    session.close();
  }

  private void createWeeklyBatch(Session session, SubmitterProfile profile)
  {
    Query query;
    Transaction tx = session.beginTransaction();
    profile.initPotentialIssueStatus(session);
    QualityCollector qualityCollector = new QualityCollector("Weekly DQA", BatchType.WEEKLY, profile);
    MessageBatch messageBatch = qualityCollector.getMessageBatch();
    messageBatch.setStartDate(startOfWeek.getTime());
    messageBatch.setEndDate(endOfWeek.getTime());
    session.save(messageBatch);
    {
      query = session
          .createQuery("from MessageBatch where profile = ? and endDate >= ? and endDate < ? and batchType = ?");
      query.setParameter(0, profile);
      query.setParameter(1, startOfWeek.getTime());
      query.setParameter(2, endOfWeek.getTime());
      query.setParameter(3, BatchType.SUBMISSION);
      List<MessageBatch> submittedBatches = query.list();
      for (MessageBatch submittedBatch : submittedBatches)
      {
        query = session.createQuery("from BatchReport where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<BatchReport> reportList = query.list();
        if (reportList.size() > 0)
        {
          submittedBatch.setBatchReport(reportList.get(0));
        }
        messageBatch.addToCounts(submittedBatch);
        query = session.createQuery("from BatchIssues where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<BatchIssues> batchIssuesList = query.list();
        for (BatchIssues batchIssues : batchIssuesList)
        {
          messageBatch.getBatchIssues(batchIssues.getIssue()).inc(batchIssues);
        }
        query = session.createQuery("from BatchActions where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<BatchActions> batchActionsList = query.list();
        for (BatchActions batchActions : batchActionsList)
        {
          messageBatch.getBatchActions(batchActions.getIssueAction()).inc(batchActions);
        }
        query = session.createQuery("from BatchCodeReceived where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<BatchCodeReceived> batchCodeReceivedList = query.list();
        for (BatchCodeReceived batchCodeReceived : batchCodeReceivedList)
        {
          messageBatch.getBatchCodeReceived(batchCodeReceived.getCodeReceived()).inc(batchCodeReceived);
        }
        query = session.createQuery("from BatchVaccineCvx where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<BatchVaccineCvx> batchVaccineCvxList = query.list();
        for (BatchVaccineCvx batchVaccineCvx : batchVaccineCvxList)
        {
          messageBatch.getBatchVaccineCvx(batchVaccineCvx.getVaccineCvx()).inc(batchVaccineCvx);
        }
        query = session.createQuery("from ReceiveQueue where messageBatch = ?");
        query.setParameter(0, submittedBatch);
        List<ReceiveQueue> receiveQueueList = query.list();
        for (ReceiveQueue receiveQueue : receiveQueueList)
        {
          ReceiveQueue newReceiveQueue = new ReceiveQueue();
          newReceiveQueue.setMessageBatch(messageBatch);
          MessageReceived messageReceived = receiveQueue.getMessageReceived();
          if (qualityCollector.getExampleHeader() == null)
          {
            query = session.createQuery("from MessageHeader where messageReceived = ?");
            query.setParameter(0, messageReceived);
            List<MessageHeader> messageHeaderList = query.list();
            if (messageHeaderList.size() > 0)
            {
              qualityCollector.setExampleHeader(messageHeaderList.get(0));
            }
          }
          
          newReceiveQueue.setMessageReceived(messageReceived);
          if (receiveQueue.getSubmitStatus().isQueued())
          {
            if (profile.isProfileStatusProd())
            {
              newReceiveQueue.setSubmitStatus(SubmitStatus.PREPARED);
            } else
            {
              newReceiveQueue.setSubmitStatus(SubmitStatus.HOLD);
            }
          } else
          {
            newReceiveQueue.setSubmitStatus(SubmitStatus.EXCLUDED);
          }
          session.save(newReceiveQueue);
          messageReceived.setSubmitStatus(newReceiveQueue.getSubmitStatus());
        }
      }
    }
    qualityCollector.score();
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_ENABLE, false))
    {
      String rootDirString = ksm.getKeyedValue(KeyedSetting.IN_FILE_DIR, "c:/data/in");
      File rootDir = new File(rootDirString);
      if (rootDir.exists() && rootDir.isDirectory() && profile.getProfileCode() != null
          && !profile.getProfileCode().equals(""))
      {
        File dqaDir = createDqaDir(profile, ksm, rootDir);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String filename = "Weekly DQA for " + profile.getProfileCode() + "." + sdf.format(endOfWeek.getTime()) + "." + getScoreDescription(messageBatch.getBatchReport().getOverallScore()) + "."
            + messageBatch.getBatchReport().getOverallScore() + ".html";
        try
        {
          PrintWriter reportOut = new PrintWriter(new FileWriter(new File(dqaDir, filename)));
          QualityReport qualityReport = new QualityReport(qualityCollector, profile, session, reportOut);
          qualityReport.printReport();
          reportOut.close();
        } catch (IOException ioe)
        {
          ioe.printStackTrace();
        }
      }
    }
    saveMessageBatch(session, profile, qualityCollector, messageBatch);
    tx.commit();
  }

  private void saveMessageBatch(Session session, SubmitterProfile profile, QualityCollector messageBatchManager,
      MessageBatch messageBatch)
  {
    if (profile.isProfileStatusProd())
    {
      messageBatch.setSubmitStatus(SubmitStatus.PREPARED);
    } else
    {
      messageBatch.setSubmitStatus(SubmitStatus.HOLD);
    }
    session.save(messageBatchManager.getMessageBatch());
    for (BatchIssues batchIssues : messageBatch.getBatchIssuesMap().values())
    {
      session.save(batchIssues);
    }
    for (BatchActions batchActions : messageBatch.getBatchActionsMap().values())
    {
      session.save(batchActions);
    }
  }

  private File createDqaDir(SubmitterProfile profile, KeyedSettingManager ksm, File rootDir)
  {
    File dir = new File(rootDir, profile.getProfileCode());
    if (!dir.exists())
    {
      dir.mkdir();
    }
    File dqaDir = new File(dir, ksm.getKeyedValue(KeyedSetting.IN_FILE_DQA_DIR_NAME, "dqa"));
    if (!dqaDir.exists())
    {
      dqaDir.mkdir();
    }
    return dqaDir;
  }


  private void determineStartAndEndOfWeek(Date now)
  {
    endOfWeek = Calendar.getInstance();
    endOfWeek.setTime(now);
    endOfWeek.clear(Calendar.HOUR_OF_DAY);
    endOfWeek.clear(Calendar.HOUR);
    endOfWeek.clear(Calendar.MINUTE);
    endOfWeek.clear(Calendar.SECOND);
    endOfWeek.clear(Calendar.MILLISECOND);
    endOfWeek.clear(Calendar.ZONE_OFFSET);
    int dayToday = endOfWeek.get(Calendar.DAY_OF_WEEK);
    if (dayToday < weekStartDay)
    {
      dayToday += 7;
    }
    int offset = weekStartDay - dayToday;
    endOfWeek.add(Calendar.DAY_OF_MONTH, offset);
    startOfWeek = Calendar.getInstance();
    startOfWeek.setTime(endOfWeek.getTime());
    startOfWeek.add(Calendar.DAY_OF_MONTH, -7);
    internalLog.append("End of week = " + sdf.format(endOfWeek.getTime()) + "\r");
    internalLog.append("Start of week = " + sdf.format(startOfWeek.getTime()) + "\r");
  }

  private void setWeeklyParameters()
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    weekStartDay = ksm.getKeyedValueInt(KeyedSetting.WEEKLY_BATCH_DAY, 1);
    processingStartTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.WEEKLY_BATCH_START_TIME, "01:00"));
    processingEndTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.WEEKLY_BATCH_END_TIME, "12:00"));
    internalLog.append("Weekly batch day = " + weekStartDay + "\r");
    internalLog.append("Processing start time = " + processingStartTime + "\r");
    internalLog.append("Processing end time = " + processingEndTime + "\r");
  }

  private String getScoreDescription(int score)
  {
    if (score >= 90)
    {
      return "Excellent";
    } else if (score >= 80)
    {
      return "Good";
    } else if (score >= 70)
    {
      return "Okay";
    } else if (score >= 60)
    {
      return "Poor";
    } else
    {
      return "Problem";
    }
  }
}
