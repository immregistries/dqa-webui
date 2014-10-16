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
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Clob;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.SoftwareVersion;
import org.openimmunizationsoftware.dqa.db.model.BatchActions;
import org.openimmunizationsoftware.dqa.db.model.BatchCodeReceived;
import org.openimmunizationsoftware.dqa.db.model.BatchIssues;
import org.openimmunizationsoftware.dqa.db.model.BatchType;
import org.openimmunizationsoftware.dqa.db.model.BatchVaccineCvx;
import org.openimmunizationsoftware.dqa.db.model.IssueAction;
import org.openimmunizationsoftware.dqa.db.model.IssueFound;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.ReceiveQueue;
import org.openimmunizationsoftware.dqa.db.model.Submission;
import org.openimmunizationsoftware.dqa.db.model.SubmitStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.parse.VaccinationParser;
import org.openimmunizationsoftware.dqa.parse.VaccinationParserFactory;
import org.openimmunizationsoftware.dqa.quality.AnalysisReport;
import org.openimmunizationsoftware.dqa.quality.QualityCollector;
import org.openimmunizationsoftware.dqa.quality.QualityReport;
import org.openimmunizationsoftware.dqa.validate.Validator;

public class ProcessorCore
{

  private static final String ASC_LINE_START = "Patient|";
  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private PrintWriter processingOut = null;
  private ManagerThread thread = null;
  private QualityCollector qualityCollector = null;
  private File ackFile;
  private File reportFile;
  private File errorsFile;
  private File analysisDir;
  private File logFile;
  private File analysisFile;
  private PrintWriter ackOut;
  private PrintWriter logOut;
  private PrintWriter reportOut;
  private PrintWriter errorsOut;
  private PrintWriter acceptedOut;
  private SubmitterProfile profile = null;
  private VaccinationParser parser = null;
  private File acceptedDir = null;
  private File receiveDir = null;
  private Submission submission = null;
  private Session session = null;

  public File getLogFile()
  {
    return logFile;
  }

  public File getAckFile()
  {
    return ackFile;
  }

  public File getReportFile()
  {
    return reportFile;
  }

  public File getErrorsFile()
  {
    return errorsFile;
  }

  public File getAnalysisDir()
  {
    return analysisDir;
  }

  public PrintWriter getProcessingOut()
  {
    return processingOut;
  }

  public void setProcessingOut(PrintWriter processingOut)
  {
    this.processingOut = processingOut;
  }

  public ManagerThread getThread()
  {
    return thread;
  }

  public void setThread(ManagerThread thread)
  {
    this.thread = thread;
  }

  public QualityCollector getQualityCollector()
  {
    return qualityCollector;
  }

  public void setQualityCollector(QualityCollector qualityCollector)
  {
    this.qualityCollector = qualityCollector;
  }

  public SubmitterProfile getProfile()
  {
    return profile;
  }

  public void setProfile(SubmitterProfile profile)
  {
    this.profile = profile;
  }

  public VaccinationParser getParser()
  {
    return parser;
  }

  public void setParser(VaccinationParser parser)
  {
    this.parser = parser;
  }

  public File getAcceptedDir()
  {
    return acceptedDir;
  }

  public void setAcceptedDir(File acceptedDir)
  {
    this.acceptedDir = acceptedDir;
  }

  public File getReceiveDir()
  {
    return receiveDir;
  }

  public void setReceiveDir(File receiveDir)
  {
    this.receiveDir = receiveDir;
  }

  public ProcessorCore(PrintWriter processingOut, ManagerThread thread, SubmitterProfile profile, File acceptedDir, File receiveDir) {
    this.processingOut = processingOut;
    this.thread = thread;
    this.profile = profile;
    this.acceptedDir = acceptedDir;
    this.receiveDir = receiveDir;
  }

  public ProcessorCore(PrintWriter processingOut, ManagerThread thread, SubmitterProfile profile, Submission submission, Session session) {
    this.processingOut = processingOut;
    this.thread = thread;
    this.profile = profile;
    this.submission = submission;
    this.session = session;
  }

  public void process(Session session, String filename, File inFile) throws FileNotFoundException, IOException
  {
    Reader inReader = new FileReader(inFile);
    processIn(session, filename, inReader);
    inFile.delete();
  }

  public void processIn(Session session, String filename, Reader inReader) throws IOException
  {

    BufferedReader in = new BufferedReader(inReader);

    procLog("Starting file processing");
    Date receivedDate = determineReceivedDate(filename);
    thread.setProgressStart(System.currentTimeMillis());
    createMessageBatch(session);

    String line = readRealFirstLine(in);
    if (line != null && (line.startsWith("MSH") || line.startsWith("FHS") || line.startsWith("BHS") || line.startsWith(ASC_LINE_START)))
    {
      String previousLineStart = "*******";
      StringBuilder message = new StringBuilder();
      openOutputs(filename);
      thread.setProgressCount(0);
      do
      {
        line = line.trim();
        if (acceptedOut != null)
        {
          acceptedOut.print(line);
          acceptedOut.print("\r");
        }
        if (line.startsWith("MSH") || (line.startsWith(ASC_LINE_START) && !line.startsWith(previousLineStart)))
        {
          if (message.length() > 0)
          {
            processMessage(message, profile, session, receivedDate);
          }
          message.setLength(0);
        } else if (line.startsWith("FHS") || line.startsWith("BHS") || line.startsWith("BTS") || line.startsWith("FTS"))
        {
          continue;
        }
        message.append(line);
        message.append("\r");
        if (line.startsWith(ASC_LINE_START))
        {
          int pos = line.indexOf('|', ASC_LINE_START.length());
          if (pos == -1)
          {
            previousLineStart = line + "|";
          } else
          {
            previousLineStart = line.substring(0, pos) + "|";
          }
        }
      } while ((line = in.readLine()) != null);

      if (message.length() > 0)
      {
        processMessage(message, profile, session, receivedDate);
      }
      printReport(filename, session);
      if (thread.getProgressCount() > 0)
      {
        procLog("Finished processing messages, saving submission batch");
        saveAndCloseBatch(session);
      } else
      {
        procLog("No messages found to process.");
      }

      closeOutputs();
    } else
    {
      procLog("File does not start with HL7 content, not processing");
    }
    in.close();
  }

  private void updateSubmissionStatus(Session session, Submission submission, String submissionStatus)
  {
    Transaction transaction = session.beginTransaction();
    submission.setSubmissionStatus(submissionStatus);
    submission.setSubmissionStatusDate(new Date());
    transaction.commit();
  }

  private void processMessage(StringBuilder message, SubmitterProfile profile, Session session, Date receivedDate)
  {
    String messageString = message.toString();
    thread.incProgressCount();
    procLog("Processing message " + thread.getProgressCount());

    if (submission != null)
    {
      updateSubmissionStatus(session, submission, Submission.SUBMISSION_STATUS_PROCESSING);
    }
    Transaction tx = session.beginTransaction();
    try
    {
      MessageReceived messageReceived = new MessageReceived();
      messageReceived.setReceivedDate(receivedDate);
      messageReceived.setProfile(profile);
      messageReceived.setRequestText(messageString);
      if (parser == null)
      {
        parser = VaccinationParserFactory.createVaccinationParser(messageString, profile);
      }
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
      if (ackOut != null)
      {
        ackOut.print(ackMessage);
      }
      if (logOut != null)
      {
        printLogDetails(message, messageReceived, logOut, false);
      }
      if (issueAction.isError())
      {
        if (errorsOut != null)
        {
          printLogDetails(message, messageReceived, errorsOut, true);
        }
        procLog(" + REJECTED ");
      }

      tx.commit();
    } catch (Throwable exception)
    {
      procLog(" + EXCEPTION: " + exception.getMessage());
      exception.printStackTrace();
      tx.rollback();
      String ackMessage = "MSH|^~\\&|||||201105231008000||ACK^|201105231008000|P|2.3.1|\r" + "MSA|AE|TODO|Exception occurred: "
          + exception.getMessage() + "|\r";
      if (ackOut != null)
      {
        ackOut.print(ackMessage);
      }
      exception.printStackTrace(processingOut);
      if (logOut != null)
      {
        exception.printStackTrace(logOut);
      }
      if (errorsOut != null)
      {
        exception.printStackTrace(errorsOut);
      }
      if (reportOut != null)
      {
        exception.printStackTrace(reportOut);
      }
    }
    session.flush();
    session.clear();
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

  private void createMessageBatch(Session session)
  {
    Transaction tx = session.beginTransaction();
    qualityCollector = new QualityCollector("File Import", BatchType.SUBMISSION, profile);
    session.save(qualityCollector.getMessageBatch());
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

  private void procLog(String message)
  {
    processingOut.println(sdf.format(new Date()) + " " + message);
    processingOut.flush();
  }

  protected void openOutputs(String filename) throws IOException
  {
    File outFile;
    if (submission == null)
    {
      outFile = new File(acceptedDir, filename);
      ackFile = new File(receiveDir, filename + ".ack.hl7");
      logFile = new File(receiveDir, filename + ".log.txt");
      reportFile = new File(receiveDir, filename + ".report.html");
      errorsFile = new File(receiveDir, filename + ".errors.txt");
      analysisDir = new File(receiveDir, filename + ".analysis");
    } else
    {
      String prefix = profile.getProfileId() + "." + submission.getRequestName();
      outFile = null;
      ackFile = submission.isReturnResponse() ? File.createTempFile(prefix, ".ack.hl7") : null;
      logFile = submission.isReturnDetailLog() ? File.createTempFile(prefix, ".log.hl7") : null;
      reportFile = submission.isReturnReport() ? File.createTempFile(prefix, ".report.hl7") : null;
      errorsFile = submission.isReturnDetailError() ? File.createTempFile(prefix, ".errors.hl7") : null;
      // analysisDir = File.createTempFile(submission.getRequestName(),
      // ".analysis.html"); new File(receiveDir, filename + ".analysis");
    }
    if (outFile != null)
    {
      acceptedOut = new PrintWriter(new FileWriter(outFile, true));
    } else
    {
      acceptedOut = null;
    }
    if (ackFile != null)
    {
      ackOut = new PrintWriter(new FileWriter(ackFile));
    } else
    {
      ackOut = null;
    }
    if (logFile != null)
    {
      logOut = new PrintWriter(new FileWriter(logFile));
    } else
    {
      logOut = null;
    }
    if (reportFile != null)
    {
      reportOut = new PrintWriter(new FileWriter(reportFile));
    } else
    {
      reportOut = null;
    }
    if (errorsFile != null)
    {
      errorsOut = new PrintWriter(new FileWriter(errorsFile));
    } else
    {
      errorsOut = null;
    }
  }

  private void printReport(String filename, Session session) throws IOException
  {
    qualityCollector.score();
    if (reportOut != null)
    {
      procLog("Creating DQA Report");
      QualityReport qualityReport = new QualityReport(qualityCollector, profile, session, reportOut);
      qualityReport.setFilename(filename);
      qualityReport.printReport();
    }
    if (analysisDir != null)
    {
      procLog("Creating Analysis Report");
      AnalysisReport analysisReport = new AnalysisReport(qualityCollector, session, profile, analysisDir);
      analysisReport.setFilename(filename);
      analysisReport.printReport();
    } else
    {
      if (submission != null && submission.isReturnAnalysis())
      {
        procLog("Creating Analysis Report");
        AnalysisReport analysisReport = new AnalysisReport(qualityCollector, session, profile, submission);
        analysisReport.setFilename(filename);
        analysisReport.printReport();
        analysisFile = analysisReport.getAnalysisFile();
      }
    }
    procLog("Finished creating reports");
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

  private void closeOutputs() throws IOException
  {
    long progressEnd = System.currentTimeMillis();
    if (logOut != null)
    {
      logOut.println("Processing Complete");
      logOut.println("Start Time:       " + sdf.format(new Date(thread.getProgressStart())));
      logOut.println("End Time:         " + sdf.format(new Date(progressEnd)));
      logOut.println("Message Count:    " + thread.getProgressCount());
      logOut.println("Message/Second:   " + ((float) thread.getProgressCount()) / ((progressEnd - thread.getProgressStart()) / 1000.0));
      logOut.println("Software Label:   " + KeyedSettingManager.getApplication().getApplicationLabel());
      logOut.println("Software Type:    " + KeyedSettingManager.getApplication().getApplicationType());
      logOut.println("Software Version: " + SoftwareVersion.VENDOR + " " + SoftwareVersion.PRODUCT + " " + SoftwareVersion.VERSION + " "
          + SoftwareVersion.BINARY_ID);
      logOut.println("Software Release: " + SoftwareVersion.RELEASE_DATE);
    }
    if (acceptedOut != null)
    {
      acceptedOut.close();
    }
    if (ackOut != null)
    {
      ackOut.close();
    }
    if (logOut != null)
    {
      logOut.close();
    }
    if (reportOut != null)
    {
      reportOut.close();
    }
    if (errorsOut != null)
    {
      errorsOut.close();
    }
    thread.setProgressStart(0);
    thread.setProgressEnd(0);
    if (submission != null)
    {
      submission.setBatch(qualityCollector.getMessageBatch());
      Transaction transaction = session.beginTransaction();
      FileReader ackReader = null;
      FileReader errorReader = null;
      FileReader logReader = null;
      FileReader reportReader = null;
      FileReader analysisReader = null;
      if (ackOut != null)
      {
        ackReader = new FileReader(ackFile);
        submission.setResponseContent(Hibernate.createClob(ackReader, ackFile.length(), session));
      }
      if (errorsOut != null)
      {
        errorReader = new FileReader(errorsFile);
        submission.setResponseDetailError(Hibernate.createClob(errorReader, errorsFile.length(), session));
      }
      if (logOut != null)
      {
        logReader = new FileReader(logFile);
        submission.setResponseDetailLog(Hibernate.createClob(logReader, logFile.length(), session));
      }
      if (reportOut != null)
      {
        reportReader = new FileReader(reportFile);
        submission.setResponseReport(Hibernate.createClob(reportReader, reportFile.length(), session));
      }
      if (analysisFile != null)
      {
        analysisReader = new FileReader(analysisFile);
        submission.setResponseAnalysis(Hibernate.createClob(analysisReader, analysisFile.length(), session));
      }
      submission.setSubmissionStatus(Submission.SUBMISSION_STATUS_FINISHED);
      submission.setSubmissionStatusDate(new Date());
      session.update(submission);
      session.flush();
      transaction.commit();
      session.flush();

      if (ackReader != null)
      {
        ackReader.close();
        ackFile.delete();
      }
      if (errorReader != null)
      {
        errorReader.close();
        errorsFile.delete();
      }
      if (logReader != null)
      {
        logReader.close();
        logFile.delete();
      }
      if (reportReader != null)
      {
        reportReader.close();
        reportFile.delete();
      }
      if (analysisReader != null)
      {
        analysisReader.close();
        analysisFile.delete();
      }

    }
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
      out.println("Message " + thread.getProgressCount());
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
        printBean(out, messageReceived, "  ", new HashSet<Object>());
      }
      out.format("Current processing speed: %.2f messages/second ",
          ((float) thread.getProgressCount()) / ((System.currentTimeMillis() - thread.getProgressStart()) / 1000.0));

      out.println();
      out.println();
    } catch (Exception e)
    {
      e.printStackTrace(out);
    }
  }

  private static List<String> printBean(PrintWriter out, Object object, String indent, Set<Object> objectsPrinted) throws IllegalAccessException,
      InvocationTargetException
  {
    List<String> thisPrinted = new ArrayList<String>();
    List<String> subPrinted = new ArrayList<String>();
    if (objectsPrinted.contains(object))
    {
      return thisPrinted;
    }
    objectsPrinted.add(object);
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
            printBean(out, list.get(i), indent + "  ", objectsPrinted);
          }
        } else
        {
          out.print(indent);
          out.println(fieldName);
          List<String> returnPrints = printBean(out, returnValue, indent + "  ", objectsPrinted);
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
