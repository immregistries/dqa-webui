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
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.ProcessLocker;
import org.openimmunizationsoftware.dqa.SoftwareVersion;
import org.openimmunizationsoftware.dqa.db.model.Application;
import org.openimmunizationsoftware.dqa.db.model.BatchActions;
import org.openimmunizationsoftware.dqa.db.model.BatchCodeReceived;
import org.openimmunizationsoftware.dqa.db.model.BatchIssues;
import org.openimmunizationsoftware.dqa.db.model.BatchType;
import org.openimmunizationsoftware.dqa.db.model.BatchVaccineCvx;
import org.openimmunizationsoftware.dqa.db.model.IssueAction;
import org.openimmunizationsoftware.dqa.db.model.IssueFound;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.ReceiveQueue;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.Submission;
import org.openimmunizationsoftware.dqa.db.model.SubmitStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.parse.VaccinationParserHL7;
import org.openimmunizationsoftware.dqa.quality.QualityCollector;
import org.openimmunizationsoftware.dqa.quality.QualityReport;
import org.openimmunizationsoftware.dqa.validate.Validator;

public class SubmissionProcessor extends ManagerThread
{

  private SubmissionManager submissionManager = null;
  private String submitterName = null;

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private PrintWriter processingOut = null;
  private File acceptedDir = null;
  private File receiveDir = null;
  private SubmitterProfile profile = null;
  private VaccinationParserHL7 parser = null;
  private PrintWriter ackOut;
  private PrintWriter logOut;
  private PrintWriter reportOut;
  private PrintWriter errorsOut;
  private PrintWriter acceptedOut;
  private QualityCollector qualityCollector = null;

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
      Clob requestContent = submission.getRequestContent();
      BufferedReader reader = new BufferedReader(requestContent.getCharacterStream());
      processorCore.processIn(session, submission.getRequestName(), reader);
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

  private void processFile(Session session, String filename, File inFile) throws FileNotFoundException, IOException
  {
    Date receivedDate = determineReceivedDate(filename);
    progressStart = System.currentTimeMillis();
    createMessageBatch(session);
    BufferedReader in = new BufferedReader(new FileReader(inFile));
    String line = readRealFirstLine(in);
    if (line != null && (line.startsWith("MSH") || line.startsWith("FHS") || line.startsWith("BHS")))
    {
      StringBuilder message = new StringBuilder();
      openOutputs(filename);
      progressCount = 0;
      do
      {
        line = line.trim();
        acceptedOut.print(line);
        acceptedOut.print("\r");
        if (line.startsWith("MSH"))
        {
          if (message.length() > 0)
          {
            progressCount++;
            processMessage(message, profile, session, receivedDate);
          }
          message.setLength(0);
        } else if (line.startsWith("FHS") || line.startsWith("BHS") || line.startsWith("BTS") || line.startsWith("FTS"))
        {
          continue;
        }
        message.append(line);
        message.append("\r");
      } while ((line = in.readLine()) != null);
      if (message.length() > 0)
      {
        progressCount++;
        processMessage(message, profile, session, receivedDate);
      }
      printReport(inFile, session);
      if (progressCount > 0)
      {
        saveAndCloseBatch(session);
      }
      closeOutputs(acceptedOut);
    }
    in.close();
    inFile.delete();
  }

  private Date determineReceivedDate(String filename)
  {
    Date receivedDate = new Date();
    int pos = filename.indexOf("!");
    if (pos != -1)
    {
      procLog("Filename appears to include received date");
      pos++;
      if (pos < filename.length())
      {
        String s = filename.substring(pos);
        pos = s.indexOf('.');
        if (pos == -1)
        {
          pos = filename.length();
        }
        if (pos == 8)
        {
          SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMdd");
          fileDate.setLenient(false);
          try
          {
            receivedDate = fileDate.parse(s.substring(0, pos));
            if (receivedDate.getTime() > System.currentTimeMillis())
            {
              procLog("Unable to set received date in the future, using today's date");
              receivedDate = new Date();
            } else
            {
              procLog("Setting received date for file to " + sdf.format(receivedDate));
            }
          } catch (ParseException pe)
          {
            procLog("Tried to set received date from file name but unable to parse date in YYYYMMDD format");
          }
        } else
        {
          procLog("Expected received date in file but was not available");
        }
      }
    }
    return receivedDate;
  }

  private void saveAndCloseBatch(Session session)
  {
    qualityCollector.close();
    Transaction tx = session.beginTransaction();
    MessageBatch messageBatch = qualityCollector.getMessageBatch();
    messageBatch.setSubmitStatus(SubmitStatus.QUEUED);
    session.saveOrUpdate(messageBatch);
    session.saveOrUpdate(messageBatch.getBatchReport());
    for (BatchIssues batchIssues : messageBatch.getBatchIssuesMap().values())
    {
      session.save(batchIssues);
    }
    for (BatchActions batchActions : messageBatch.getBatchActionsMap().values())
    {
      session.save(batchActions);
    }
    for (BatchCodeReceived batchCodeReceived : messageBatch.getBatchCodeReceivedMap().values())
    {
      session.save(batchCodeReceived);
    }
    for (BatchVaccineCvx batchVaccineCvx : messageBatch.getBatchVaccineCvxMap().values())
    {
      session.save(batchVaccineCvx);
    }
    tx.commit();
  }

  private void createMessageBatch(Session session)
  {
    Transaction tx = session.beginTransaction();
    qualityCollector = new QualityCollector("File Import", BatchType.SUBMISSION, profile);
    session.save(qualityCollector.getMessageBatch());
    tx.commit();
  }

  private void printReport(File inFile, Session session) throws IOException
  {
    Transaction tx = session.beginTransaction();
    qualityCollector.score();
    QualityReport qualityReport = new QualityReport(qualityCollector, profile, session, reportOut);
    qualityReport.setFilename(inFile.getName());
    qualityReport.printReport();
    tx.commit();
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

  private void openOutputs(String filename) throws IOException
  {
    File outFile = new File(acceptedDir, filename);
    acceptedOut = new PrintWriter(new FileWriter(outFile, true));
    ackOut = new PrintWriter(new FileWriter(new File(receiveDir, filename + ".ack.hl7")));
    logOut = new PrintWriter(new FileWriter(new File(receiveDir, filename + ".log.txt")));
    reportOut = new PrintWriter(new FileWriter(new File(receiveDir, filename + ".report.html")));
    errorsOut = new PrintWriter(new FileWriter(new File(receiveDir, filename + ".errors.txt")));
  }

  private void closeOutputs(PrintWriter acceptedOut)
  {
    long progressEnd = System.currentTimeMillis();
    logOut.println("Processing Complete");
    logOut.println("Start Time:       " + sdf.format(new Date(progressStart)));
    logOut.println("End Time:         " + sdf.format(new Date(progressEnd)));
    logOut.println("Message Count:    " + progressCount);
    logOut.println("Message/Second:   " + ((float) progressCount) / ((progressEnd - progressStart) / 1000.0));
    logOut.println("Software Label:   " + KeyedSettingManager.getApplication().getApplicationLabel());
    logOut.println("Software Type:    " + KeyedSettingManager.getApplication().getApplicationType());
    logOut.println("Software Version: " + SoftwareVersion.VENDOR + " " + SoftwareVersion.PRODUCT + " " + SoftwareVersion.VERSION + " "
        + SoftwareVersion.BINARY_ID);
    acceptedOut.close();
    ackOut.close();
    logOut.close();
    reportOut.close();
    errorsOut.close();
    progressStart = 0;
    progressCount = 0;
  }

  private void processMessage(StringBuilder message, SubmitterProfile profile, Session session, Date receivedDate)
  {
    Transaction tx = session.beginTransaction();
    try
    {
      MessageReceived messageReceived = new MessageReceived();
      messageReceived.setReceivedDate(receivedDate);
      messageReceived.setProfile(profile);
      messageReceived.setRequestText(message.toString());

      parser.createVaccinationUpdateMessage(messageReceived);
      if (!messageReceived.hasErrors())
      {
        Validator validator = new Validator(profile, session);
        validator.validateVaccinationUpdateMessage(messageReceived, qualityCollector);
      }
      IssueAction issueAction = determineIssueAction(messageReceived);
      messageReceived.setIssueAction(issueAction);
      qualityCollector.registerProcessedMessage(messageReceived);

      String ackMessage = parser.makeAckMessage(messageReceived);
      messageReceived.setResponseText(ackMessage);
      MessageReceivedManager.saveMessageReceived(profile, messageReceived, session);
      saveInQueue(session, messageReceived);
      // profile.saveCodesReceived(session);
      ackOut.print(ackMessage);
      printLogDetails(message, messageReceived, logOut, false);
      if (issueAction.isError())
      {
        printLogDetails(message, messageReceived, errorsOut, true);
      }
      tx.commit();
    } catch (Throwable exception)
    {
      tx.rollback();
      String ackMessage = "MSH|^~\\&|||||201105231008000||ACK^|201105231008000|P|2.3.1|\r" + "MSA|AE|TODO|Exception occurred: "
          + exception.getMessage() + "|\r";
      ackOut.print(ackMessage);
      exception.printStackTrace(processingOut);
      exception.printStackTrace(logOut);
      exception.printStackTrace(errorsOut);
      exception.printStackTrace(reportOut);
    }
    session.flush();
    session.clear();
  }

  private IssueAction determineIssueAction(MessageReceived messageReceived)
  {
    IssueAction issueAction;
    if (messageReceived.getPatient().isSkipped())
    {
      issueAction = IssueAction.SKIP;
    } else if (messageReceived.hasErrors())
    {
      issueAction = IssueAction.ERROR;
    } else if (messageReceived.hasWarns())
    {
      issueAction = IssueAction.WARN;
    } else
    {
      issueAction = IssueAction.ACCEPT;
    }
    return issueAction;
  }

  private void saveInQueue(Session session, MessageReceived messageReceived)
  {
    ReceiveQueue receiveQueue = new ReceiveQueue();
    receiveQueue.setMessageBatch(qualityCollector.getMessageBatch());
    receiveQueue.setMessageReceived(messageReceived);
    receiveQueue.setSubmitStatus(messageReceived.hasErrors() ? SubmitStatus.EXCLUDED : SubmitStatus.QUEUED);
    messageReceived.setSubmitStatus(receiveQueue.getSubmitStatus());
    session.save(receiveQueue);
  }

  private void printLogDetails(StringBuilder message, MessageReceived messageReceived, PrintWriter out, boolean printDetails)
  {
    try
    {
      out.println("Message " + progressCount);
      out.println(message);
      List<IssueFound> issuesFound = messageReceived.getIssuesFound();
      boolean first = true;
      for (IssueFound issueFound : issuesFound)
      {
        if (issueFound.isError())
        {
          if (first)
          {
            out.println("Errors:");
            first = false;
          }
          out.println(" + " + issueFound.getDisplayText());
        }
      }
      first = true;
      for (IssueFound issueFound : issuesFound)
      {
        if (issueFound.isWarn())
        {
          if (first)
          {
            out.println("Warnings:");
            first = false;
          }
          out.println(" + " + issueFound.getDisplayText());
        }
      }
      first = true;
      for (IssueFound issueFound : issuesFound)
      {
        if (issueFound.isSkip())
        {
          if (first)
          {
            out.println("Skip:");
            first = false;
          }
          out.println(" + " + issueFound.getDisplayText());
        }
      }
      if (printDetails)
      {
        out.println("Message Data: ");
        printBean(out, messageReceived, "  ");
      }
      out.format("Current processing speed: %.2f messages/second", ((float) progressCount) / ((System.currentTimeMillis() - progressStart) / 1000.0));

      out.println();
      out.println();
    } catch (Exception e)
    {
      e.printStackTrace(out);
    }
  }

  private static List<String> printBean(PrintWriter out, Object object, String indent) throws IllegalAccessException, InvocationTargetException
  {
    List<String> thisPrinted = new ArrayList<String>();
    List<String> subPrinted = new ArrayList<String>();
    Method[] methods = object.getClass().getMethods();
    Arrays.sort(methods, new Comparator<Method>() {
      public int compare(Method o1, Method o2)
      {
        return o1.getName().compareTo(o2.getName());
      }
    });
    for (Method method : methods)
    {
      if (method.getName().startsWith("get") && !method.getName().equals("getClass") && !method.getName().equals("getMessageReceived")
          && !method.getName().equals("getProfile") && !method.getName().equals("getTableType") && !method.getName().equals("getCodeReceived")
          && !method.getName().equals("getRequestText") && !method.getName().equals("getResponseText") && !method.getName().equals("getIssuesFound")
          && !method.getReturnType().equals(Void.TYPE) && method.getParameterTypes().length == 0)
      {
        Object returnValue = method.invoke(object);
        String fieldName = method.getName().substring(3);
        if (returnValue == null || subPrinted.contains(fieldName))
        {
          // do nothing
        } else if (method.getReturnType() == String.class)
        {
          if (!((String) returnValue).equals(""))
          {
            out.print(indent);
            out.print(fieldName);
            out.print(" = '");
            out.print(returnValue);
            out.println("'");
            thisPrinted.add(fieldName);
          }
        } else if (method.getReturnType() == Date.class)
        {
          out.print(indent);
          out.print(fieldName);
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          out.print(" = ");
          out.println(sdf.format((Date) returnValue));
          thisPrinted.add(fieldName);
        } else if (method.getReturnType() == List.class)
        {
          List list = (List) returnValue;
          for (int i = 0; i < list.size(); i++)
          {
            out.print(indent);
            out.print(fieldName);
            out.print(" #");
            out.println(i + 1);
            printBean(out, list.get(i), indent + "  ");
          }
        } else
        {
          out.print(indent);
          out.println(fieldName);
          List<String> returnPrints = printBean(out, returnValue, indent + "  ");
          for (String returnPrint : returnPrints)
          {
            subPrinted.add(fieldName + returnPrint);
          }
        }
      }
    }
    return thisPrinted;
  }

}
