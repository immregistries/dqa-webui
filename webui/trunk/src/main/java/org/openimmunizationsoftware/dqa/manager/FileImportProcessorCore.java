package org.openimmunizationsoftware.dqa.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

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
import org.openimmunizationsoftware.dqa.db.model.SubmitStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.parse.VaccinationUpdateParserHL7;
import org.openimmunizationsoftware.dqa.quality.QualityCollector;
import org.openimmunizationsoftware.dqa.quality.QualityReport;
import org.openimmunizationsoftware.dqa.validate.Validator;

public class FileImportProcessorCore
{

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private PrintWriter processingOut = null;
  private ManagerThread thread = null;
  private QualityCollector qualityCollector = null;
  private File ackFile;
  private File reportFile;
  private File errorsFile;
  private File logFile;
  private PrintWriter ackOut;
  private PrintWriter logOut;
  private PrintWriter reportOut;
  private PrintWriter errorsOut;
  private PrintWriter acceptedOut;
  private SubmitterProfile profile = null;
  private VaccinationUpdateParserHL7 parser = null;
  private File acceptedDir = null;
  private File receiveDir = null;

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

  public PrintWriter getAckOut()
  {
    return ackOut;
  }

  public void setAckOut(PrintWriter ackOut)
  {
    this.ackOut = ackOut;
  }

  public PrintWriter getLogOut()
  {
    return logOut;
  }

  public void setLogOut(PrintWriter logOut)
  {
    this.logOut = logOut;
  }

  public PrintWriter getReportOut()
  {
    return reportOut;
  }

  public void setReportOut(PrintWriter reportOut)
  {
    this.reportOut = reportOut;
  }

  public PrintWriter getErrorsOut()
  {
    return errorsOut;
  }

  public void setErrorsOut(PrintWriter errorsOut)
  {
    this.errorsOut = errorsOut;
  }

  public PrintWriter getAcceptedOut()
  {
    return acceptedOut;
  }

  public void setAcceptedOut(PrintWriter acceptedOut)
  {
    this.acceptedOut = acceptedOut;
  }

  public SubmitterProfile getProfile()
  {
    return profile;
  }

  public void setProfile(SubmitterProfile profile)
  {
    this.profile = profile;
  }

  public VaccinationUpdateParserHL7 getParser()
  {
    return parser;
  }

  public void setParser(VaccinationUpdateParserHL7 parser)
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

  public FileImportProcessorCore(PrintWriter processingOut, ManagerThread thread, SubmitterProfile profile, VaccinationUpdateParserHL7 parser,
      File acceptedDir, File receiveDir) {
    this.processingOut = processingOut;
    this.thread = thread;
    this.parser = parser;
    this.profile = profile;
    this.acceptedDir = acceptedDir;
    this.receiveDir = receiveDir;
  }

  public void processFile(Session session, String filename, File inFile) throws FileNotFoundException, IOException
  {
    procLog("Starting file processing");
    Date receivedDate = determineReceivedDate(filename);
    thread.setProgressStart(System.currentTimeMillis());
    createMessageBatch(session);
    BufferedReader in = new BufferedReader(new FileReader(inFile));
    String line = readRealFirstLine(in);
    if (line != null && (line.startsWith("MSH") || line.startsWith("FHS") || line.startsWith("BHS")))
    {
      StringBuilder message = new StringBuilder();
      openOutputs(filename);
      thread.setProgressCount(0);
      do
      {
        line = line.trim();
        acceptedOut.print(line);
        acceptedOut.print("\r");
        if (line.startsWith("MSH"))
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
      } while ((line = in.readLine()) != null);
      if (message.length() > 0)
      {
        processMessage(message, profile, session, receivedDate);
      }
      printReport(inFile, session);
      if (thread.getProgressCount() > 0)
      {
        procLog("Finished processing messages, saving submission batch");
        saveAndCloseBatch(session);
      } else
      {
        procLog("No messages found to process.");
      }
      closeOutputs(acceptedOut);
    } else
    {
      procLog("File does not start with HL7 content, not processing");
    }
    in.close();
    inFile.delete();
  }

  private void processMessage(StringBuilder message, SubmitterProfile profile, Session session, Date receivedDate)
  {
    thread.incProgressCount();
    procLog("Processing message " + thread.getProgressCount());

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
        procLog(" + REJECTED ");
      }
      
      tx.commit();
    } catch (Throwable exception)
    {
      procLog(" + EXCEPTION: " + exception.getMessage());
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

  private void openOutputs(String filename) throws IOException
  {
    File outFile = new File(acceptedDir, filename);
    acceptedOut = new PrintWriter(new FileWriter(outFile, true));
    ackFile = new File(receiveDir, filename + ".ack.hl7");
    logFile = new File(receiveDir, filename + ".log.txt");
    reportFile = new File(receiveDir, filename + ".report.html");
    errorsFile = new File(receiveDir, filename + ".errors.txt");
    ackOut = new PrintWriter(new FileWriter(ackFile));
    logOut = new PrintWriter(new FileWriter(logFile));
    reportOut = new PrintWriter(new FileWriter(reportFile));
    errorsOut = new PrintWriter(new FileWriter(errorsFile));
  }

  private void printReport(File inFile, Session session)
  {
    Transaction tx = session.beginTransaction();
    qualityCollector.score();
    QualityReport qualityReport = new QualityReport(qualityCollector, profile, reportOut);
    qualityReport.setFilename(inFile.getName());
    qualityReport.printReport();
    tx.commit();
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

  private void closeOutputs(PrintWriter acceptedOut)
  {
    long progressEnd = System.currentTimeMillis();
    logOut.println("Processing Complete");
    logOut.println("Start Time:       " + sdf.format(new Date(thread.getProgressStart())));
    logOut.println("End Time:         " + sdf.format(new Date(progressEnd)));
    logOut.println("Message Count:    " + thread.getProgressCount());
    logOut.println("Message/Second:   " + ((float) thread.getProgressCount()) / ((progressEnd - thread.getProgressStart()) / 1000.0));
    logOut.println("Software Label:   " + KeyedSettingManager.getApplication().getApplicationLabel());
    logOut.println("Software Type:    " + KeyedSettingManager.getApplication().getApplicationType());
    logOut.println("Software Version: " + SoftwareVersion.VENDOR + " " + SoftwareVersion.PRODUCT + " " + SoftwareVersion.VERSION + " "
        + SoftwareVersion.BINARY_ID);
    acceptedOut.close();
    ackOut.close();
    logOut.close();
    reportOut.close();
    errorsOut.close();
    thread.setProgressStart(0);
    thread.setProgressEnd(0);
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
        printBean(out, messageReceived, "  ");
      }
      out.format("Current processing speed: %.2f messages/second ", ((float) thread.getProgressCount())
          / ((System.currentTimeMillis() - thread.getProgressStart()) / 1000.0));

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
