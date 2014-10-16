/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.ProcessLocker;
import org.openimmunizationsoftware.dqa.db.model.Application;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.Submission;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;

public class SubmissionProcessor extends ManagerThread
{

  private SubmissionManager submissionManager = null;
  private String submitterName = null;

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private PrintWriter processingOut = null;
  private SubmitterProfile profile = null;

  protected SubmissionProcessor(String submitterName, SubmissionManager fileImportManager) {
    super("Submission Processor for " + submitterName);
    this.submissionManager = fileImportManager;
    this.submitterName = submitterName;
  }

  @Override
  public void run()
  {
    try
    {
      internalLog.append("Looking for submissions to process \n");
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      processingOut = new PrintWriter(System.out); // TODO find out what
                                                   // to do with output
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();
      findProfile(submitterName, session);

      Transaction tx = session.beginTransaction();
      profile.initPotentialIssueStatus(session);
      tx.commit();
      Query query = session.createQuery("from Submission where submitterName = ? and submissionStatus = ? order by createdDate ASC");
      query.setParameter(0, submitterName);
      query.setParameter(1, Submission.SUBMISSION_STATUS_SUBMITTED);
      List<Submission> submissionList = query.list();
      procLog("Found " + submissionList.size() + " submissions to process");
      internalLog.append("Found " + submissionList.size() + " submissions to process");
      for (Submission submission : submissionList)
      {
        lookToProcessSubmission(session, submission);
      }
      session.close();
    } catch (Exception e)
    {
      e.printStackTrace();
      submissionManager.lastException = e;
    } finally
    {
      // Processing is complete, now remove thread and complete
      submissionManager.threads.remove(submitterName);
    }

  }

  private void findProfile(String profileCode, Session session)
  {
    procLog("Looking for submitter profile");
    Query query = session.createQuery("from SubmitterProfile where profileCode = ?");
    query.setParameter(0, profileCode);
    List<SubmitterProfile> submitterProfiles = query.list();
    if (submitterProfiles.size() == 0)
    {
      procLog("Submitter profile not found, creating new one");
      Transaction tx = session.beginTransaction();
      Organization organization = new Organization();
      organization.setOrgLabel(profileCode);
      organization.setParentOrganization((Organization) session.get(Organization.class, 1));
      profile = new SubmitterProfile();
      profile.setProfileLabel("HL7 File");
      profile.setProfileStatus(SubmitterProfile.PROFILE_STATUS_TEST);
      profile.setOrganization(organization);
      profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
      profile.setTransferPriority(SubmitterProfile.TRANSFER_PRIORITY_NORMAL);
      profile.setProfileCode(profileCode);
      profile.generateAccessKey();
      query = session.createQuery("from Application where runThis = 'Y'");
      List<Application> applicationList = query.list();
      if (applicationList.size() > 0)
      {
        Application application = applicationList.get(0);
        profile.setReportTemplate(application.getPrimaryReportTemplate());
      } else
      {
        profile.setReportTemplate((ReportTemplate) session.get(ReportTemplate.class, 1));
      }
      session.save(organization);
      session.save(profile);
      organization.setPrimaryProfile(profile);
      tx.commit();
    } else
    {
      profile = submitterProfiles.get(0);
      procLog("Using profile " + profile.getProfileId() + " '" + profile.getProfileLabel() + "' for organization '"
          + profile.getOrganization().getOrgLabel() + "'");
    }

  }

  private void lookToProcessSubmission(Session session, Submission submission) throws FileNotFoundException, IOException
  {
    procLog("Looking at submission " + submission.getRequestName());
    internalLog.append("Looking at submission " + submission.getRequestName());
    // if (fileCanBeProcessed(inFile))
    // {
    procLog("Processing file " + submission.getRequestName());
    internalLog.append("  + processing file... ");
    try
    {
      ProcessLocker.lock(profile);
      submission.setProfile(profile);
      updateSubmissionStatus(session, submission, Submission.SUBMISSION_STATUS_PROCESSING);
      ProcessorCore processorCore = new ProcessorCore(processingOut, this, profile, submission, session);
      if (submissionCanBeProcessed(submission, session))
      {
        Clob requestContent = submission.getRequestContent();
        BufferedReader reader = new BufferedReader(requestContent.getCharacterStream());
        processorCore.processIn(session, submission.getRequestName(), reader);
      }
    } catch (SQLException sqle)
    {
      updateSubmissionStatus(session, submission, Submission.SUBMISSION_STATUS_ERROR);
      throw new IOException("Unable to read clob", sqle);
    } finally
    {
      ProcessLocker.unlock(profile);
    }
    // } else
    // {
    // procLog("File does not contain processable data or is not complete: " +
    // filename);
    // }
  }

  /**
   * Read the file before processing and ensure it looks like what we expect.
   * First non blank line should be file header segment or message header
   * segment. In addition if there is a file header segment then the last non
   * blank line is expected to be the trailing segment. Otherwise the file is
   * assumed to contain HL7 messages. It is important to note that this check
   * does not validate HL7 format, but is built to ensure that the entire file
   * has been transmitted when batch header/footers are sent and that the file
   * doesn't contain obvious non-HL7 content.
   * 
   * @param inFile
   * @return
   * @throws IOException
   */
  private boolean submissionCanBeProcessed(Submission submission, Session session) throws SQLException, IOException
  {
    String errorMessage = null;
    HashSet<String> messageControlIdHash = null;
    HashSet<String> messageControlIdNotUniqueHash = null;
    boolean verifyUniqueControlId = false;
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (ksm.getKeyedValueBoolean(KeyedSetting.IN_SUBMISSION_VERIFY_UNIQUE_CONTROL_ID, false))
    {
      messageControlIdHash = new HashSet<String>();
      messageControlIdNotUniqueHash = new HashSet<String>();
      verifyUniqueControlId = true;
    }

    Clob requestContent = submission.getRequestContent();
    BufferedReader reader = new BufferedReader(requestContent.getCharacterStream());
    String line = readRealFirstLine(reader);
    boolean okay;
    boolean startsWithFHS = false;
    if (line.startsWith("FHS"))
    {
      startsWithFHS = true;
      okay = true;
    } else if (line.startsWith("MSH"))
    {
      okay = true;
    } else if (line.startsWith("Patient|"))
    {
      okay = true;
    } else
    {
      okay = false;
      errorMessage = "Submission does not appear to be HL7, expecting to start with an FHS or MSH segment.";
    }
    if (okay)
    {
      if (startsWithFHS || verifyUniqueControlId)
      {
        String lastLine = line;
        while ((line = reader.readLine()) != null)
        {
          if (line.trim().length() > 0)
          {
            if (verifyUniqueControlId && line.startsWith("MSH") && line.length() > 9)
            {
              char sepChar = line.charAt(3);
              String[] fields = line.split("\\" + sepChar);
              if (fields.length > 9)
              {
                String messageControlId = fields[9];
                if (messageControlId != null && messageControlId.length() > 0)
                {
                  if (messageControlIdHash.contains(messageControlId))
                  {
                    messageControlIdNotUniqueHash.add(messageControlId);
                  } else
                  {
                    messageControlIdHash.add(messageControlId);
                  }
                }
              }
            }
          }
          lastLine = line;
        }
        if (verifyUniqueControlId)
        {
          okay = messageControlIdNotUniqueHash.size() == 0;
          errorMessage = "Message control ids are not unique within this single submission.";
        }
        if (startsWithFHS)
        {
          if (!lastLine.startsWith("FTS"))
          {
            okay = false;
            errorMessage = "Submission does not end with FTS segment, submission appears to be incomplete.";
          }
        }
      }
    }
    reader.close();
    if (!okay)
    {
      String prefix = profile.getProfileId() + "." + submission.getRequestName();
      File ackFile = submission.isReturnResponse() ? File.createTempFile(prefix, ".ack.hl7") : null;
      PrintWriter ackOut = new PrintWriter(new FileWriter(ackFile));
      ackOut.println("Submission will not be processed because: " + errorMessage);
      if (verifyUniqueControlId && messageControlIdNotUniqueHash.size() > 0)
      {
        ackOut.println("The following control ids have been incorrectly re-used within this submission:");
        for (String messageControlId : messageControlIdNotUniqueHash)
        {
          ackOut.println(" + " + messageControlId);
        }
        ackOut.println("HL7 standard requires that the sender use a unique control id for each message sent.");
        ackOut.println("Submissions with duplicate control ids will not be accepted. ");
      }
      ackOut.close();
      Transaction transaction = session.beginTransaction();
      FileReader ackReader = new FileReader(ackFile);
      submission.setResponseContent(Hibernate.createClob(ackReader, ackFile.length(), session));
      submission.setSubmissionStatus(Submission.SUBMISSION_STATUS_ERROR);
      submission.setSubmissionStatusDate(new Date());
      session.update(submission);
      session.flush();
      transaction.commit();
      ackFile.delete();
    }

    return okay;
  }

  /**
   * Reads the first lines of the file until it comes to a non empty line. This
   * is to handle situations where the first few lines are empty and the HL7
   * message does not immediately start.
   * 
   * @param in
   * @return
   * @throws IOException
   */
  private String readRealFirstLine(BufferedReader in) throws IOException
  {
    String line = null;
    while ((line = in.readLine()) != null)
    {
      line = line.trim();
      if (line.length() > 0)
      {
        break;
      }
    }
    return line;
  }

  private void updateSubmissionStatus(Session session, Submission submission, String submissionStatus)
  {
    Transaction transaction = session.beginTransaction();
    submission.setSubmissionStatus(submissionStatus);
    submission.setSubmissionStatusDate(new Date());
    transaction.commit();
  }

  private void procLog(String message)
  {
    processingOut.println(sdf.format(new Date()) + " " + message);
    processingOut.flush();
  }

}
