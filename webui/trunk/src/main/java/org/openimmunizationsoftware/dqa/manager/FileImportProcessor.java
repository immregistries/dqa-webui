package org.openimmunizationsoftware.dqa.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
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
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.MessageBatch;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.ReceiveQueue;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.SubmitStatus;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.parse.VaccinationUpdateParserHL7;
import org.openimmunizationsoftware.dqa.quality.QualityCollector;
import org.openimmunizationsoftware.dqa.quality.QualityReport;
import org.openimmunizationsoftware.dqa.validate.Validator;

public class FileImportProcessor extends ManagerThread
{

  private FileImportManager fileImportManager = null;
  private File dir;

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private File processingFile = null;
  private PrintWriter processingOut = null;
  private File acceptedDir = null;
  private File receiveDir = null;
  private File submitDir = null;
  private SubmitterProfile profile = null;
  private VaccinationUpdateParserHL7 parser = null;
  private PrintWriter ackOut;
  private PrintWriter logOut;
  private PrintWriter reportOut;
  private PrintWriter errorsOut;
  private PrintWriter acceptedOut;
  private String profileCode = null;
  private QualityCollector qualityCollector = null;

  protected FileImportProcessor(File dir, FileImportManager fileImportManager) {
    super("File Import Processor for " + dir.getName());
    this.fileImportManager = fileImportManager;
    this.dir = dir;
  }

  @Override
  public void run()
  {
    try
    {
      internalLog.append("Looking for files to process \n");
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      processingFile = new File(dir, "processing.txt");
      processingOut = new PrintWriter(new FileWriter(processingFile));
      createProcessingDirs(ksm, dir);
      profileCode = dir.getName();
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();
      findProfile(profileCode, session);
      if (ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_EXPORT_CONNECTION_SCRIPT, false))
      {
        File connectionScriptFile = new File(dir, "connectionScript.txt");
        PrintWriter out = new PrintWriter(new FileWriter(connectionScriptFile));
        out.println("-----------------------------------------");
        out.println("Connection");
        out.println("Label: " + profile.getProfileLabel());
        out.println("Type: POST");
        out.println("URL: " + ksm.getKeyedValue(KeyedSetting.APPLICATION_EXTERNAL_URL_BASE, "http://localhost:8281/") + "in");
        out.println("User Id: " + profile.getProfileCode());
        out.println("Password: " + profile.getAccessKey());
        out.println("Facility Id: " + profile.getProfileId());
        out.close();
      }
      Transaction tx = session.beginTransaction();
      profile.initPotentialIssueStatus(session);
      tx.commit();
      String[] filesToProcess = submitDir.list(new FilenameFilter() {
        public boolean accept(java.io.File dir, String name)
        {
          File file = new File(dir, name);
          return file.isFile() && file.canRead();
        }
      });
      procLog("Found " + filesToProcess.length + " files to process in " + submitDir.getAbsolutePath());
      internalLog.append("Found " + filesToProcess.length + " files to process in " + submitDir.getAbsolutePath());
      for (String filename : filesToProcess)
      {
        lookToProcessFile(session, filename);
      }
      session.close();
      renameProcessLogfile(dir);
    } catch (Exception e)
    {
      e.printStackTrace();
      fileImportManager.lastException = e;
    } finally
    {
      // Processing is complete, now remove thread and complete
      fileImportManager.threads.remove(dir.getName());
    }

  }

  private void createProcessingDirs(KeyedSettingManager ksm, File dir)
  {
    acceptedDir = new File(dir, ksm.getKeyedValue(KeyedSetting.IN_FILE_ACCEPTED_DIR_NAME, "accepted"));
    if (!acceptedDir.exists())
    {
      procLog("Processed directory does not exist, creating");
      acceptedDir.mkdir();
    }
    receiveDir = new File(dir, ksm.getKeyedValue(KeyedSetting.IN_FILE_RECEIVE_DIR_NAME, "receive"));
    if (!receiveDir.exists())
    {
      procLog("Received directory does not exist, creating");
      receiveDir.mkdir();
    }
    submitDir = new File(dir, ksm.getKeyedValue(KeyedSetting.IN_FILE_SUBMIT_DIR_NAME, "submit"));
    if (!submitDir.exists())
    {
      procLog("Submit directory does not exist, creating");
      submitDir.mkdir();
    }
  }

  private void renameProcessLogfile(File dir)
  {
    procLog("processing complete");
    processingOut.close();
    File processedFile = new File(dir, "processed.txt");
    if (processedFile.exists())
    {
      processedFile.delete();
    }
    processingFile.renameTo(processedFile);
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
    parser = new VaccinationUpdateParserHL7(profile);
  }

  private void lookToProcessFile(Session session, String filename) throws FileNotFoundException, IOException
  {
    File inFile = new File(submitDir, filename);
    procLog("Looking at file " + filename);
    internalLog.append("Looking at file " + filename);
    if (inFile.exists() && inFile.canRead() && inFile.length() > 0)
    {
      long timeSinceLastChange = System.currentTimeMillis() - inFile.lastModified();
      if (timeSinceLastChange > (60 * 1000))
      {
        if (fileContainsHL7(inFile))
        {
          procLog("Processing file " + filename);
          internalLog.append("  + processing file... ");
          try
          {
            ProcessLocker.lock(profile);
            FileImportProcessorCore fileImportProcessorCore = new FileImportProcessorCore(processingOut, this, profile, parser, acceptedDir,
                receiveDir);
            fileImportProcessorCore.processFile(session, filename, inFile);
          } finally
          {
            ProcessLocker.unlock(profile);
          }
        }
      } else
      {
        procLog("Postponing processing, file changed " + (timeSinceLastChange / 1000) + " seconds ago");
        procLog(" + current time: " + sdf.format(new Date()));
        procLog(" + changed time: " + sdf.format(inFile.lastModified()));
        internalLog.append(" + postponing processing, file recently changed");
      }
    } else
    {
      procLog("File does not exist or can not be read");
      internalLog.append(" + file does not exist or can not be read");
    }
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
  private boolean fileContainsHL7(File inFile) throws IOException
  {
    BufferedReader in = new BufferedReader(new FileReader(inFile));
    String line = readRealFirstLine(in);
    boolean okay;
    if (line.startsWith("FHS"))
    {
      String lastLine = line;
      while ((line = in.readLine()) != null)
      {
        if (line.trim().length() > 0)
        {
          lastLine = line;
        }
      }
      okay = lastLine.startsWith("FTS");
      if (!okay)
      {
        procLog("ERROR: File does not end with FTS segment as expected, not processing");
      }
    } else if (line.startsWith("MSH"))
    {
      procLog("WARNING: File does not start with FHS segment as expected. ");
      okay = true;
    } else
    {
      okay = false;
      procLog("ERROR: File does not appear to contain HL7. Must start with FHS or MSH segment.");
    }
    in.close();
    return okay;
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

  private void printReport(File inFile, Session session)
  {
    Transaction tx = session.beginTransaction();
    qualityCollector.score();
    QualityReport qualityReport = new QualityReport(qualityCollector, profile, reportOut);
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
      out.println("Current processing speed: " + ((float) progressCount) / ((System.currentTimeMillis() - progressStart) / 1000.0));

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
