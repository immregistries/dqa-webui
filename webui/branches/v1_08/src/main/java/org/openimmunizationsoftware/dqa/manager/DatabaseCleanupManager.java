/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.BatchActions;
import org.openimmunizationsoftware.dqa.db.model.BatchCodeReceived;
import org.openimmunizationsoftware.dqa.db.model.BatchIssues;
import org.openimmunizationsoftware.dqa.db.model.BatchReport;
import org.openimmunizationsoftware.dqa.db.model.BatchVaccineCvx;
import org.openimmunizationsoftware.dqa.db.model.IssueFound;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.ReceiveQueue;
import org.openimmunizationsoftware.dqa.db.model.Submission;
import org.openimmunizationsoftware.dqa.db.model.SubmissionAnalysis;
import org.openimmunizationsoftware.dqa.db.model.received.NextOfKin;
import org.openimmunizationsoftware.dqa.db.model.received.Patient;
import org.openimmunizationsoftware.dqa.db.model.received.Vaccination;
import org.openimmunizationsoftware.dqa.db.model.received.VaccinationVIS;
import org.openimmunizationsoftware.dqa.db.model.received.types.PatientAddress;
import org.openimmunizationsoftware.dqa.db.model.received.types.PatientIdNumber;
import org.openimmunizationsoftware.dqa.db.model.received.types.PatientImmunity;
import org.openimmunizationsoftware.dqa.db.model.received.types.PatientPhone;

public class DatabaseCleanupManager extends ManagerThread
{

  private static DatabaseCleanupManager singleton = null;

  public static synchronized DatabaseCleanupManager getDatabaseCleanupManager()
  {
    if (singleton == null)
    {
      singleton = new DatabaseCleanupManager();
      singleton.start();
    }
    return singleton;
  }

  private DatabaseCleanupManager() {
    super("DatabaseCleanupManager");
  }

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");

  private int databaseCleanupDay;
  private Date processingStartTime;
  private Date processingEndTime;

  private boolean enabled = false;
  private int dayOfWeekToday = 0;

  private Date deleteBeforeDataFields = null;
  private Date deleteBeforeMessageText = null;
  private Date deleteBeforeMessageAnalysis = null;
  private Date deleteBeforeBatchReportsSubmitted = null;
  private Date deleteBeforeBatchReportsWeekly = null;
  private Date deleteBeforeSubmissions = null;

  @Override
  public void run()
  {
    internalLog.append("Starting database cleanup manager\r");
    while (keepRunning)
    {
      try
      {
        setCleanupParameters();
        Date now = new Date();
        if (enabled && (dayOfWeekToday == databaseCleanupDay) && processingStartTime.before(now) && processingEndTime.after(now))
        {
          runNow(now);
        } else
        {
          internalLog.append("Database Cleanup will not run now\r");
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
    setDeleteBefore(now);
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();

    deleteMessageTexts(session);
    deleteDataFields(session);
    deleteMessageAnalysis(session);
    deleteBatchReports(session, deleteBeforeBatchReportsWeekly);
    deleteBatchReports(session, deleteBeforeBatchReportsSubmitted);
    {
      int count = 0;
      do
      {
        count = 0;
        Query query = session.createQuery("from Submission where submissionStatusDate < ?");
        query.setParameter(0, deleteBeforeSubmissions);
        List<Submission> submissionList = query.list();
        for (Submission submission : submissionList)
        {
          Transaction tx = session.beginTransaction();
          query = session.createQuery("from SubmissionAnalysis where submission = ?");
          query.setParameter(0, submission);
          List<SubmissionAnalysis> submissionAnalysisList = query.list();
          for (SubmissionAnalysis submissionAnalysis : submissionAnalysisList)
          {
            session.delete(submissionAnalysis);
          }
          session.delete(submission);

          tx.commit();
          count++;
        }
        internalLog.append("Deleted " + count + " submissions \r");
      } while (count > 0);
    }

    session.close();
  }

  public void deleteBatchReports(Session session, Date deleteBefore)
  {
    int count = 0;
    do
    {
      count = 0;
      Query query = session.createQuery("from MessageBatch where endDate < ?");
      query.setParameter(0, deleteBefore);
      query.setMaxResults(1000);
      List<MessageBatch> messageBatchList = query.list();
      for (MessageBatch messageBatch : messageBatchList)
      {
        Transaction tx = session.beginTransaction();

        query = session.createQuery("from ReceiveQueue where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<ReceiveQueue> receiveQueueList = query.list();
        for (ReceiveQueue receiveQueue : receiveQueueList)
        {
          session.delete(receiveQueue);
        }

        query = session.createQuery("from BatchIssues where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<BatchIssues> batchIssuesList = query.list();
        for (BatchIssues batchIssues : batchIssuesList)
        {
          session.delete(batchIssues);
        }

        query = session.createQuery("from BatchCodeReceived where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<BatchCodeReceived> batchCodeReceivedList = query.list();
        for (BatchCodeReceived batchCodeReceived : batchCodeReceivedList)
        {
          session.delete(batchCodeReceived);
        }

        query = session.createQuery("from BatchActions where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<BatchActions> batchActionsList = query.list();
        for (BatchActions batchActions : batchActionsList)
        {
          session.delete(batchActions);
        }

        query = session.createQuery("from BatchVaccineCvx where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<BatchVaccineCvx> batchVaccineCvxList = query.list();
        for (BatchVaccineCvx batchVaccineCvx : batchVaccineCvxList)
        {
          session.delete(batchVaccineCvx);
        }

        query = session.createQuery("from BatchReport where messageBatch = ? ");
        query.setParameter(0, messageBatch);
        List<BatchReport> batchReportList = query.list();
        for (BatchReport batchReportCvx : batchReportList)
        {
          session.delete(batchReportCvx);
        }

        session.delete(messageBatch);

        tx.commit();
        count++;
      }
      internalLog.append("Deleted " + count + " batch reports \r");
    } while (count > 0);
  }

  public void deleteMessageAnalysis(Session session)
  {
    int count = 0;
    do
    {
      count = 0;
      Query query = session.createQuery("from MessageReceived where receivedDate < ?");
      query.setParameter(0, deleteBeforeMessageAnalysis);
      query.setMaxResults(1000);
      List<MessageReceived> messageReceivedList = query.list();
      count = 0;
      for (MessageReceived messageReceived : messageReceivedList)
      {

        deleteMessageData(session, messageReceived);

        Transaction tx = session.beginTransaction();
        query = session.createQuery("from IssueFound where messageReceived = ? ");
        query.setParameter(0, messageReceived);
        List<IssueFound> issueFoundList = query.list();
        for (IssueFound issueFound : issueFoundList)
        {
          session.delete(issueFound);
        }

        query = session.createQuery("from ReceiveQueue where messageReceived = ? ");
        query.setParameter(0, messageReceived);
        List<ReceiveQueue> receiveQueueList = query.list();
        for (ReceiveQueue receiveQueue : receiveQueueList)
        {
          session.delete(receiveQueue);
        }

        tx = session.beginTransaction();
        session.delete(messageReceived);
        tx.commit();

        count++;
      }
      internalLog.append("Deleted " + count + " messages received\r");
    } while (count > 0);
  }

  public void deleteMessageTexts(Session session)
  {
    int count = 0;
    do
    {
      count = 0;
      Query query = session.createQuery("from MessageReceived where receivedDate < ? and requestText is not null");
      query.setParameter(0, deleteBeforeMessageText);
      query.setMaxResults(1000);
      List<MessageReceived> messageReceivedList = query.list();
      count = 0;
      for (MessageReceived messageReceived : messageReceivedList)
      {
        Transaction tx = session.beginTransaction();
        messageReceived.setRequestText(null);
        messageReceived.setResponseText(null);
        session.update(messageReceived);
        tx.commit();
        count++;
      }
      internalLog.append("Deleted " + count + " message texts received\r");
    } while (count > 0);
  }

  public void deleteDataFields(Session session)
  {
    int count = 0;
       do
    {
      count = 0;
      Query query = session.createQuery("from Patient where messageReceived.receivedDate < ?");
      query.setParameter(0, deleteBeforeDataFields);
      query.setMaxResults(1000);
      List<Patient> patientList = query.list();
      count = 0;
      for (Patient patient : patientList)
      {
        deleteMessageData(session, patient.getMessageReceived());
        count++;
      }
      internalLog.append("Deleted " + count + " data received\r");
    } while (count > 0);
  }

  public void deleteMessageData(Session session, MessageReceived messageReceived)
  {
    Query query;
    Transaction tx = session.beginTransaction();
    
    query = session.createQuery("from NextOfKin where messageReceived = ?");
    query.setParameter(0, messageReceived);
    List<NextOfKin> nextOfKinList = query.list();
    for (NextOfKin nextOfKin : nextOfKinList)
    {
      session.delete(nextOfKin);
    }

    tx.commit();
    
    query = session.createQuery("from Vaccination where messageReceived = ?");
    query.setParameter(0, messageReceived);
    List<Vaccination> vaccinationList = query.list();
    for (Vaccination vaccination : vaccinationList)
    {
      deleteVaccination(session, vaccination);
    }

    query = session.createQuery("from Patient where messageReceived = ?");
    query.setParameter(0, messageReceived);
    List<Patient> patientList = query.list();
    for (Patient patient : patientList)
    {
      deletePatient(session, patient);
    }
  }

  public void deletePatient(Session session, Patient patient)
  {
    Query query;
    Transaction tx = session.beginTransaction();
    query = session.createQuery("from PatientIdNumber where patient = ?");
    query.setParameter(0, patient);
    List<PatientIdNumber> patientIdNumberList = query.list();
    for (PatientIdNumber patientIdNumber : patientIdNumberList)
    {
      session.delete(patientIdNumber);
    }
    query = session.createQuery("from PatientPhone where patient = ?");
    query.setParameter(0, patient);
    List<PatientPhone> patientPhoneList = query.list();
    for (PatientPhone patientPhone : patientPhoneList)
    {
      session.delete(patientPhone);
    }
    query = session.createQuery("from PatientAddress where patient = ?");
    query.setParameter(0, patient);
    List<PatientAddress> patientAddressList = query.list();
    for (PatientAddress patientAddress : patientAddressList)
    {
      session.delete(patientAddress);
    }
    query = session.createQuery("from PatientImmunity where patient = ?");
    query.setParameter(0, patient);
    List<PatientImmunity> patientImmunityList = query.list();
    for (PatientImmunity patientImmunity : patientImmunityList)
    {
      session.delete(patientImmunity);
    }

    session.delete(patient);
    tx.commit();
  }

  public void deleteVaccination(Session session, Vaccination vaccination)
  {
    Query query;
    Transaction tx = session.beginTransaction();
    query = session.createQuery("from VaccinationVIS where vaccination = ? ");
    query.setParameter(0, vaccination);
    List<VaccinationVIS> vaccinationVISList = query.list();
    for (VaccinationVIS vaccinationVIS : vaccinationVISList)
    {
      session.delete(vaccinationVIS);
    }
    session.delete(vaccination);
    tx.commit();
  }

  private void setDeleteBefore(Date now)
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    deleteBeforeDataFields = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_DATA_FIELDS_AFTER_DAYS, 30);
    deleteBeforeMessageText = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_MESSAGE_TEXT_AFTER_DAYS, 60);
    deleteBeforeMessageAnalysis = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_MESSAGE_ANALYSIS_AFTER_DAYS, 150);
    deleteBeforeBatchReportsSubmitted = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_BATCH_REPORTS_SUBMITTED_AFTER_DAYS, 400);
    deleteBeforeBatchReportsWeekly = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_BATCH_REPORTS_WEEKLY_AFTER_DAYS, 1500);
    deleteBeforeSubmissions = createDeleteBeforeDate(ksm, KeyedSetting.DATABASE_CLEANUP_SUBMISSIONS_AFTER_DAYS, 30);

    internalLog.append("Delete Data Fields submitted before = " + sdf.format(deleteBeforeDataFields) + "\r");
    internalLog.append("Delete Message Text submitted before = " + sdf.format(deleteBeforeMessageText) + "\r");
    internalLog.append("Delete Message Analysis submitted before = " + sdf.format(deleteBeforeMessageAnalysis) + "\r");
    internalLog.append("Delete Submitted Batch Reports created before = " + sdf.format(deleteBeforeBatchReportsSubmitted) + "\r");
    internalLog.append("Delete Weekly Batch Reports created before = " + sdf.format(deleteBeforeBatchReportsWeekly) + "\r");
    internalLog.append("Delete Submissions last updated before = " + sdf.format(deleteBeforeSubmissions) + "\r");

  }

  public Date createDeleteBeforeDate(KeyedSettingManager ksm, String field, int valueDefault)
  {
    int afterDays = ksm.getKeyedValueInt(field, valueDefault);
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DAY_OF_MONTH, -afterDays);
    Date date = calendar.getTime();
    return date;
  }

  private void setCleanupParameters()
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    enabled = ksm.getKeyedValueBoolean(KeyedSetting.DATABASE_CLEANUP_ENABLED, false);
    if (enabled)
    {
      databaseCleanupDay = ksm.getKeyedValueInt(KeyedSetting.DATABASE_CLEANUP_DAY, 6);
      processingStartTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.DATABASE_CLEANUP_START_TIME, "01:00"));
      processingEndTime = getTimeToday(ksm.getKeyedValue(KeyedSetting.DATABASE_CLEANUP_END_TIME, "12:00"));
      internalLog.append("Database cleanup day = " + databaseCleanupDay + "\r");
      dayOfWeekToday = Calendar.getInstance().get(Calendar.DAY_OF_WEEK);
      if (dayOfWeekToday == databaseCleanupDay)
      {
        internalLog.append("Today is database cleanup day \r");
        internalLog.append("Processing start time = " + processingStartTime + "\r");
        internalLog.append("Processing end time = " + processingEndTime + "\r");
      } else
      {
        internalLog.append("Today is NOT database cleanup day \r");
      }
    } else
    {
      internalLog.append("Database cleanup not enabled \r");
    }
  }

}
