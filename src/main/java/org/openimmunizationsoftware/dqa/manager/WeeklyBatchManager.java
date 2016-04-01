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
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeeklyBatchManager extends ManagerThread
{
  private static final Logger LOGGER = LoggerFactory.getLogger(WeeklyBatchManager.class);
  private static WeeklyBatchManager singleton = null;

  public static synchronized WeeklyBatchManager getWeeklyBatchManager()
  {
    if (singleton == null)
    {
      singleton = new WeeklyBatchManager();
      }
    return singleton;
  }

  private WeeklyBatchManager() {
    super("WeeklyBatchManager");
  }

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
  private SimpleDateFormat longDateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss.SSS a");

  private int weekStartDay;
  private Date processingStartTime;
  private Date processingEndTime;

  private Calendar endOfWeek;
  private Calendar startOfWeek;
  private String currentLogDate;
  
  private void internalLogging(String message) {
	  internalLog.append(message);
	  fileLogger(message);
  }
  
  private void fileLogger(String message) {
	  message = "[" + currentLogDate + "] " + message;
	  LOGGER.info(message);
  }
  
  @Override
  public void runForDate(Date now) {
	  try {
			synchronized (this) {
				internalLog.setLength(0);
			  	internalLogging("Initiating weekly batch manager\r");
			  	executeProcess(now);
				internalLogging("Finishing weekly batch manager\r");
			}
		} catch (Throwable e) {
			e.printStackTrace();
			internalLogging("Weekly batch manager finished with errors\r");
			lastException = e;
		}
  }
  
  private void executeProcess(Date reportDate) 
  {
	long start = Calendar.getInstance().getTimeInMillis();
	this.currentLogDate = longDateFormat.format(new Date());
	internalLogging("Starting weekly batch manager. Execution DateTime[" + this.currentLogDate + "]\r");
	internalLogging("Running for date: " + sdf.format(reportDate) + "\r");
	setWeeklyParameters();
    determineStartAndEndOfWeek(reportDate);
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    try {
	    Transaction tx = session.beginTransaction();
	    Query query = session.createQuery("from SubmitterProfile where (profile_status = 'Prod' or profile_status = 'Test' or profile_status = 'Hold') and profile_id >= 1200");
	    List<SubmitterProfile> profiles = query.list();
	    tx.commit();
	    
	    //Grab the ID's out of the objects, and clear the list. 
	    List<Integer> profileIds = new ArrayList<Integer>();
	    for (SubmitterProfile profile : profiles) {
	    	profileIds.add(profile.getProfileId());
	    }
	    profiles = null;
	    session.flush();
	    session.clear();
	    
	    //Each submitter profile gets its own transaction, and the session is cleared after each one. 
	    for (Integer profileId : profileIds)
	    {
	      tx = session.beginTransaction();
	      
	      //Re-get the submitter profile within this session context so that the report can access some of the lazy-loaded info. 
	      SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, profileId);
	      
	      internalLogging("Looking at profile " + profile.getProfileLabel() + " for " + profile.getProfileCode() + "\r");
	      query = session.createQuery("from MessageBatch where profile = :profile and batchType = :type and endDate = :endDate");
	      query.setParameter("profile", profile);
	      query.setParameter("type", BatchType.WEEKLY);
	      query.setParameter("endDate", endOfWeek.getTime());
	      List<MessageBatch> messageBatchList = query.list();
	      tx.commit();
	      if (messageBatchList.size() == 0)
	      {
	        internalLogging(" + Creating batch\r");
	        // batch has not yet been created. Create one now
	        // Get all messages received in the week
	        
	        createWeeklyBatch(session, profile);
	        
	        //work is done for this pin! Flush and clear to free the memory. 
	        session.flush();
	        session.clear();
	      } else
	      {
	        internalLogging(" + No batches to create\r");
	      }
	    }
    } finally {
    	session.close();
    }
    internalLogging("Processing complete for report date " + sdf.format(reportDate) + "\r");
    fileLogger("--------------------[" + this.currentLogDate + "]------------------------");
    long finish = Calendar.getInstance().getTimeInMillis();
    internalLogging("Weekly Batch Process Elapsed time: " + ((finish - start)/1000) + " seconds");
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
          .createQuery("from MessageBatch where profile = :param1 and endDate >= :param2 and endDate < :param3 and batchType = :param4");
      query.setParameter("param1", profile);
      query.setParameter("param2", startOfWeek.getTime());
      query.setParameter("param3", endOfWeek.getTime());
      query.setParameter("param4", BatchType.SUBMISSION);
      List<MessageBatch> submittedBatches = query.list();
      for (MessageBatch submittedBatch : submittedBatches)
      {
        query = session.createQuery("from BatchReport where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<BatchReport> reportList = query.list();
        if (reportList.size() > 0)
        {
          submittedBatch.setBatchReport(reportList.get(0));
        }
        messageBatch.addToCounts(submittedBatch);
        query = session.createQuery("from BatchIssues where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<BatchIssues> batchIssuesList = query.list();
        for (BatchIssues batchIssues : batchIssuesList)
        {
          messageBatch.getBatchIssues(batchIssues.getIssue()).inc(batchIssues);
        }
        query = session.createQuery("from BatchActions where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<BatchActions> batchActionsList = query.list();
        for (BatchActions batchActions : batchActionsList)
        {
          messageBatch.getBatchActions(batchActions.getIssueAction()).inc(batchActions);
        }
        query = session.createQuery("from BatchCodeReceived where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<BatchCodeReceived> batchCodeReceivedList = query.list();
        for (BatchCodeReceived batchCodeReceived : batchCodeReceivedList)
        {
          messageBatch.getBatchCodeReceived(batchCodeReceived.getCodeReceived()).inc(batchCodeReceived);
        }
        query = session.createQuery("from BatchVaccineCvx where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<BatchVaccineCvx> batchVaccineCvxList = query.list();
        for (BatchVaccineCvx batchVaccineCvx : batchVaccineCvxList)
        {
          messageBatch.getBatchVaccineCvx(batchVaccineCvx.getVaccineCvx()).inc(batchVaccineCvx);
        }
        query = session.createQuery("from ReceiveQueue where messageBatch = :param1");
        query.setParameter("param1", submittedBatch);
        List<ReceiveQueue> receiveQueueList = query.list();
        for (ReceiveQueue receiveQueue : receiveQueueList)
        {
          ReceiveQueue newReceiveQueue = new ReceiveQueue();
          newReceiveQueue.setMessageBatch(messageBatch);
          MessageReceived messageReceived = receiveQueue.getMessageReceived();
          if (qualityCollector.getExampleHeader() == null)
          {
            query = session.createQuery("from MessageHeader where messageReceived = :param1");
            query.setParameter("param1", messageReceived);
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
    fileLogger("about to score the batch");
    qualityCollector.score();
    fileLogger("scoring done");
    
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_ENABLE, false))
    {
      String rootDirString = ksm.getKeyedValue(KeyedSetting.IN_FILE_DIR, "c:/data/in");
      File rootDir = new File(rootDirString);
      if (rootDir.exists() && rootDir.isDirectory() && profile.getProfileCode() != null
          && !profile.getProfileCode().equals(""))
      {
        File dqaDir = createDqaDir(profile, ksm, rootDir);
        SimpleDateFormat inputformat = new SimpleDateFormat("yyyyMMdd");
        String filename = "Weekly DQA for " + profile.getProfileCode() + "." + inputformat.format(endOfWeek.getTime()) + "." + getScoreDescription(messageBatch.getBatchReport().getOverallScore()) + "."
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
    internalLogging("End of week = " + sdf.format(endOfWeek.getTime()) + "\r");
    internalLogging("Start of week = " + sdf.format(startOfWeek.getTime()) + "\r");
  }

  private void setWeeklyParameters()
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    weekStartDay = ksm.getKeyedValueInt(KeyedSetting.WEEKLY_BATCH_DAY, 1);
    processingStartTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.WEEKLY_BATCH_START_TIME, "01:00"));
    processingEndTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.WEEKLY_BATCH_END_TIME, "12:00"));
    internalLogging("Weekly batch day = " + weekStartDay + "\r");
    internalLogging("Processing start time = " + processingStartTime + "\r");
    internalLogging("Processing end time = " + processingEndTime + "\r");
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
