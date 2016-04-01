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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.ProcessLocker;
import org.openimmunizationsoftware.dqa.db.model.Application;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.util.HibernateSessionReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileImportProcessor extends ManagerThread
{
  private static final Logger LOGGER = LoggerFactory.getLogger(FileImportProcessor.class);
  private FileImportManager fileImportManager = null;
  private File rootDir;

  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private File processingFile = null;
  private PrintWriter processingOut = null;
  private File acceptedDir = null;
  private File receiveDir = null;
  private File submitDir = null;
  private SubmitterProfile profile = null;
  private String profileCode = null;

	private int numberOffilesToProcess = 0;
	private int currentFileNumber = 0;
	private String currentFileName = "";
	
	private boolean active = false;
	private boolean complete = false;
	private boolean completedWithErrors = false;
	private int totalMessages = 0;
	private long allFilesAverage;
	
  protected FileImportProcessor(File dir, FileImportManager fileImportManager) {
    super("File Import Processor for " + dir.getName());
    LOGGER.info("Creating FileImportProcessor for directory : " + dir.getAbsolutePath());
    this.fileImportManager = fileImportManager;
    this.rootDir = dir;
  }

  @Override
  public void run()
  {
	this.setActive(true);
	this.setThreadStartMs(System.currentTimeMillis());
	try
    {
      LOGGER.info(this.rootDir.getName() + " Initiating File Import");
      dualLogging("Looking for files to process \n");
      
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      processingFile = new File(rootDir, "processing.txt");
      processingOut = new PrintWriter(new FileWriter(processingFile));
      createProcessingDirs(ksm, rootDir);
      profileCode = rootDir.getName();
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();
      findProfile(profileCode, session);
      if (ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_EXPORT_CONNECTION_SCRIPT, false))
      {
        File connectionScriptFile = new File(rootDir, "connectionScript.txt");
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
      LOGGER.info("Init PotentialIssueStatus: START");
      profile.initPotentialIssueStatus(session);
      LOGGER.info("Init PotentialIssueStatus: COMPLETE");
      tx.commit();
      String[] filesToProcess = submitDir.list(new FilenameFilter() {
        public boolean accept(java.io.File dir, String name)
        {
          File file = new File(dir, name);
          return file.isFile() && file.canRead();
        }
      });
      
      this.setNumberOffilesToProcess(filesToProcess.length);
      
      procLog("Found " + filesToProcess.length + " files to process in " + submitDir.getAbsolutePath());
      dualLogging("Found " + filesToProcess.length + " files to process in " + submitDir.getAbsolutePath());
      
      for (String filename : filesToProcess)
      {
    	LOGGER.info("Start Processing file: " + filename);
    	this.currentFileName = filename;
    	this.currentFileNumber ++;
        
    	lookToProcessFile(session, filename);
    	
    	LOGGER.info("Completed Processing file: " + filename);
    	
      }
      session.close();
      
      renameProcessLogfile(rootDir);
      
      HibernateSessionReporter.reportSingletonSessionFactoryStats();
      
    } catch (Exception e)
    {
    	LOGGER.error("Exception processing directory " + this.rootDir.getAbsolutePath(), e);
        fileImportManager.lastException = e;
        this.setCompletedWithErrors(true);
    }  
	
	this.setComplete(true);
   	this.setCompletedWithErrors(this.lastException != null);
    this.setThreadFinishMs(System.currentTimeMillis());
    this.fileImportManager.addToMessageAverage(this.totalMessages, this.allFilesAverage);

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
    Query query = session.createQuery("from SubmitterProfile where profileCode = :param1");
    query.setParameter("param1", profileCode);
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

  private void lookToProcessFile(Session session, String filename) throws FileNotFoundException, IOException
  {
    File inFile = new File(submitDir, filename);
    procLog("Looking at file " + filename);
    dualLogging("Looking at file " + filename);
    if (inFile.exists() && inFile.canRead() && inFile.length() > 0)
    {
      long timeSinceLastChange = System.currentTimeMillis() - inFile.lastModified();
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      if (timeSinceLastChange > (ksm.getKeyedValueInt(KeyedSetting.IN_SUBMISSION_WAIT, 60) * 1000))
      {
        if (fileCanBeProcessed(inFile))
        {
          procLog("Processing file " + filename);
          dualLogging("  + processing file... ");
          try
          {
            ProcessLocker.lock(profile);
            ProcessorCore pc = new ProcessorCore(processingOut, this, profile, acceptedDir, receiveDir);
            
            pc.processFile(session, filename, inFile);

            int currentMessages =  pc.getMessageCount();
            
            
            long totalPrevMessageTime = this.getTotalMessages() * this.allFilesAverage;          
            long currentMessageTime = pc.getMessageCount() * this.getAverageProcessingMs();
            
            this.setTotalMessages(this.getTotalMessages() + currentMessages);
            this.allFilesAverage = (totalPrevMessageTime + currentMessageTime) / this.getTotalMessages();
          } finally
          {
            ProcessLocker.unlock(profile);
          }
        } else
        {
          procLog("File does not contain processable data or is not complete: " + filename);
        }
      } else
      {
        procLog("Postponing processing, file changed " + (timeSinceLastChange / 1000) + " seconds ago (will process after 60 seconds has elapsed)");
        procLog(" + current time: " + sdf.format(new Date()));
        procLog(" + changed time: " + sdf.format(inFile.lastModified()));
        dualLogging(" + postponing processing, file recently changed");
      }
    } else
    {
      procLog("File does not exist or can not be read");
      dualLogging(" + file does not exist or can not be read");
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
  private boolean fileCanBeProcessed(File inFile) throws IOException
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
    } else if (line.startsWith("Patient|"))
    {
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
    LOGGER.debug("PROC LOG: " + sdf.format(new Date()) + " " + message);
  }
  
  private void dualLogging(String message) {
	  LOGGER.info(message);
	  internalLog.append(message);
  }
  
  public String getStatus() {
		String transferInterfacePin = this.rootDir.getName();
		
		if (this.isActive() && !this.isComplete()) {
//			+ 1200-75-58 RUNNING: File[5 of 75] Name[1200-75-58-20150510.lst] Message[8 of  15]
			if (this.progressStart == 0) {
				return String.format(" + %10s STARTING ", transferInterfacePin);
			}
			
			long avgElapsed = this.getAverageProcessingMs();		
			long threadElapsed = System.currentTimeMillis() - this.getThreadStartMs();
			return String.format(" + %10s RUNNING: File[%3d of %3d] Name[%23s] Message[%3d of  %3d] MsgAvg[%6d ms]  thread elapsed[%10d s]", transferInterfacePin, this.currentFileNumber, this.getNumberOffilesToProcess(), this.currentFileName, this.getProgressCount(), this.getTotalCount(), avgElapsed, threadElapsed / 1000);
		} if (this.isComplete()) {
			return String.format(" + %s PROCESSING COMPLETE - Files[%3d] Messages[%3d] MsgAvg[%6d ms] Elapsed[%10d s]  %s", transferInterfacePin, this.getNumberOffilesToProcess(), this.getTotalMessages(), this.allFilesAverage, ((this.getThreadFinishMs() - this.getThreadStartMs())/1000), (this.completedWithErrors ? " (WITH ERRORS) " : ""));
		} else {
			return String.format(" + %s WAITING IN THREAD POOL", transferInterfacePin);
		}
		
	}
  
  /**
   * @return the active
   */
  public boolean isActive() {
  	return active;
  }

  /**
   * @param active the active to set
   */
  public void setActive(boolean active) {
  	this.active = active;
  }

  /**
   * @return the complete
   */
  public boolean isComplete() {
  	return complete;
  }

  /**
   * @param complete the complete to set
   */
  public void setComplete(boolean complete) {
  	this.complete = complete;
  }

  /**
   * @return the completedWithErrors
   */
  public boolean isCompletedWithErrors() {
  	return completedWithErrors;
  }

  /**
   * @param completedWithErrors the completedWithErrors to set
   */
  public void setCompletedWithErrors(boolean completedWithErrors) {
  	this.completedWithErrors = completedWithErrors;
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

/**
 * @return the numberOffilesToProcess
 */
public int getNumberOffilesToProcess() {
	return numberOffilesToProcess;
}

/**
 * @param numberOffilesToProcess the numberOffilesToProcess to set
 */
public void setNumberOffilesToProcess(int numberOffilesToProcess) {
	this.numberOffilesToProcess = numberOffilesToProcess;
}

/**
 * @return the totalMessages
 */
public int getTotalMessages() {
	return totalMessages;
}

/**
 * @param totalMessages the totalMessages to set
 */
public void setTotalMessages(int totalMessages) {
	this.totalMessages = totalMessages;
}


}