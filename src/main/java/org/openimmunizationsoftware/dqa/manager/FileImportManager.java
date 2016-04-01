/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.manager;

import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.wicket.util.file.File;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.quality.MetaDataRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileImportManager extends ManagerThreadMulti {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileImportManager.class);
	private static final FileImportManager SINGLETON = new FileImportManager();

	private static final FilenameFilter DIRECTORY_FILE_FILTER = new FilenameFilter() {
		public boolean accept(java.io.File dir, String name) {
			return new File(dir, name).isDirectory();
		}
	};

	public static synchronized FileImportManager getFileImportManager() {
		LOGGER.info("Getting FileImportManager instance.");
		return SINGLETON;
	}

	private final boolean importEnabled;
	private final int maxThreadCount;
	private final String submitDirectoryName;
	private final String keyedRootDirString;

	private List<FileImportProcessor> threadList;
	private static String DQA_IN_FILE_DIR = "dqa.in.file.dir";
	private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
	private StringBuilder directoryLog = new StringBuilder();
	private StringBuilder executionLog = new StringBuilder();

	private StringBuilder completedMessages;
	private AtomicInteger totalFiles = new AtomicInteger(0);
	private AtomicInteger totalMessages = new AtomicInteger(0);
	private AtomicInteger avgMsgMs = new AtomicInteger(0);
	
	private FileImportManager() {
		super("File Import Manager");

		if (SINGLETON != null) {
			throw new IllegalStateException(
					"Singleton is already instantitated");
		}
		KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();

		importEnabled = ksm.getKeyedValueBoolean(KeyedSetting.IN_FILE_ENABLE, false);
		maxThreadCount = ksm.getKeyedValueInt(
				KeyedSetting.IN_FILE_THREAD_COUNT_MAX, 5);
		submitDirectoryName = ksm.getKeyedValue(
				KeyedSetting.IN_FILE_SUBMIT_DIR_NAME, "submit");
		keyedRootDirString = ksm.getKeyedValue(KeyedSetting.IN_FILE_DIR,
				"c:/data/in");
		
		// processWaitTime = ksm.getKeyedValueInt(KeyedSetting.IN_FILE_WAIT, 15)
		// * 1000;
		LOGGER.warn("FileImportManager singleton has not been initialized yet.  Now creating. ");

		// Initialize resources.
		internalLog.setLength(0);
		threadList = new ArrayList<FileImportProcessor>();
		this.completedMessages = new StringBuilder();
	}
	
	@Override
	public StringBuilder getInternalLog() {
		
		List<FileImportProcessor> threadListCopy = new ArrayList<FileImportProcessor>();
		threadListCopy.addAll(threadList);
		StringBuilder sb = new StringBuilder();

		if (this.executionLog.length() == 0 && this.internalLog.length() == 0) {
			sb.append("FILE IMPORT PROCESS HAS NOT BEEN INITIATED. \r");
			sb.append(" + to start a session, click this link: <a href=\"file_processor?start=true\">initiate file import process</a>\r");
			sb.append("\r\r");
		}

		if (this.executionLog.length() > 0) {
			sb.append("PAST FILE IMPORTS:");
			sb.append("\r\r");
			sb.append(this.executionLog.toString());
			sb.append("\r\r");
			sb.append("\r\r");
		}

		if (this.internalLog.length() > 0) {
			sb.append("MOST RECENT FILE IMPORT:");
			sb.append("\r\r");
			sb.append(this.internalLog.toString());
		}

		if (threadListCopy.size() > 0) {
			reportAndRemoveCompletedThreads(threadListCopy);
		}

		if (threadListCopy.size() > 0) {
			sb.append("Current run time: "
					+ getDuration(System.currentTimeMillis()
							- this.getThreadStartMs()));
			sb.append("\r\r\r");
			sb.append("CURRENT THREAD POOL PROCESSING:");
			sb.append("\r\r");

			for (FileImportProcessor fip : threadListCopy) {
				if (!fip.isComplete()) {
					sb.append(fip.getStatus());
					sb.append(" \r");
				}
			}
		}

		if (this.completedMessages.length() > 0) {
			sb.append("\r\r");
			sb.append("COMPLETED THREADS:");
			sb.append("\r\r");
			sb.append(this.completedMessages.toString());
		}

		if (directoryLog.length() > 0) {
			sb.append("\r\r");
			sb.append("DIRECTORY SCAN RESULTS:");
			sb.append("\r\r");
			sb.append(directoryLog.toString());
		}

		return sb;

	}

	private void reportAndRemoveCompletedThreads(List<FileImportProcessor> threadListCopy) {
		
		if (threadList.contains(null)) {
			threadList.removeAll(Collections.singleton(null));
		}
		
		List<FileImportProcessor> removeList = new ArrayList<FileImportProcessor>();
		
		for (FileImportProcessor fip : threadListCopy) {
			if (fip.isComplete()) {
				if (completedMessages == null) {
					completedMessages = new StringBuilder();
				}

				this.completedMessages.append(fip.getStatus());
				this.completedMessages.append(" \r");
				
				this.totalFiles.addAndGet(fip.getNumberOffilesToProcess());
				this.totalMessages.addAndGet(fip.getTotalMessages());
				
				removeList.add(fip);
			}
		}
		
		try {
			if (removeList.size() > 0) {
				threadList.removeAll(removeList);
			}
		} catch (ConcurrentModificationException cme) {
			LOGGER.warn("Concurrent modification.  Threads are coming in fast! This may cause wierd stats to show.  This will have to be done the next time through", cme);
		}
	}

	private String getDuration(long millis) {
		long days = TimeUnit.MILLISECONDS.toDays(millis);
		millis -= TimeUnit.DAYS.toMillis(days);
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

		StringBuilder sb = new StringBuilder(64);

		if (hours > 0) {
			sb.append(String.format("%2d", hours));
			sb.append("h ");
		}

		if (minutes > 0) {
			sb.append(String.format("%2d", minutes));
			sb.append("m ");
		}

		sb.append(String.format("%2d", seconds));
		sb.append("s ");

		return (sb.toString());
	}

	@Override
	public void run() {
		LOGGER.info("Starting the File Import Manager Thread!");
		
		initiateImport();
	}

	public synchronized void initiateImport() {
		// Initialize resources for a new run of the job:
		
		LOGGER.info("Refreshing MetaDataRepo OrganizationMetadata: START");
		MetaDataRepo.INSTANCE.refreshOrganizationMetadata();
		LOGGER.info("Refreshing MetaDataRepo OrganizationMetadata: COMPLETE");
		
		internalLog.setLength(0);
		directoryLog.setLength(0);
		completedMessages.setLength(0);
		this.totalFiles.set(0);
		this.totalMessages.set(0);
		ExecutorService managerThreadPool = Executors
				.newFixedThreadPool(maxThreadCount);

		threadLogging("Starting file import manager, loading settings");
		threadLogging("Looking to import at " + sdf.format(new Date()) + "");
		this.setThreadStartMs(System.currentTimeMillis());

		try {
			if (importEnabled) {
				threadLogging("Import enabled");
				threadLogging("Looking in system settings first for path");
				String rootDirString = System.getProperty(DQA_IN_FILE_DIR);

				if (rootDirString == null || rootDirString.equals("")) {
					threadLogging("Not found in system settings, looking in keyed settings");
					rootDirString = keyedRootDirString;
				}

				File rootDir = new File(rootDirString);
				if (rootDir.exists() && rootDir.isDirectory()) {
					threadLogging("Root dir " + rootDirString
							+ " exists, begin processing");
					//THREAD LIST MAKER:
					submitDirectoryTasks(rootDir, managerThreadPool);
				} else {
					threadLogging("Can't find root directory: " + rootDirString
							+ "");
				}
			}
		} catch (Throwable e) {
			LOGGER.error(
					"Exception attempting to execute a file import. "
							+ e.getMessage(), e);
			lastException = e;
		}

		// This is important because it blocks the current thread until all the
		// worker threads are done in the thread pool.
		managerThreadPool.shutdown();

		try {
			managerThreadPool.awaitTermination(Long.MAX_VALUE,
					TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			LOGGER.error(
					"Interruption while waiting for threads to complete in run method of FileImportManager",
					e);
		}
		
		//This whole business of copying is necessary to avoid a concurrent modification exception. 
		List<FileImportProcessor> threadListCopy = new ArrayList<FileImportProcessor>();
		threadListCopy.addAll(threadList);
		this.reportAndRemoveCompletedThreads(threadListCopy);

		this.setThreadFinishMs(System.currentTimeMillis());

		String duration = getDuration(this.getThreadFinishMs()
				- this.getThreadStartMs());
		String endTime = sdf.format(new Date());

		this.threadLogging("Processing complete at " + endTime + "");
		this.executionLog.append(String.format("+ Completed at[%s]  files[%3d] messages[%4d] MsgAvg[%5d ms] elapsed time[%s]\r"
				, endTime
				, this.getTotalFiles().get()
				, this.getTotalMessages().get()
				, this.avgMsgMs.get()
				, duration
				));
		
		this.threadLogging("Total run time: " + duration);
		this.threadLogging("Total Files   : " + this.getTotalFiles());
		this.threadLogging("Total Messages: " + this.getTotalMessages());

		// Report Completed threads so they can get removed and garbage
		// collected.

	}

	private void threadLogging(String message) {
		LOGGER.info(message);
		internalLog.append(message + "\r");
	}

	private void dirLogging(String message) {
		directoryLog.append(message + "\r");
		LOGGER.info(message);
	}

	FilenameFilter getDirectoryFilter() {
		return DIRECTORY_FILE_FILTER;
	}

	String[] getSubDirectories(File parentDirectory) {
		return parentDirectory.list(getDirectoryFilter());
	}

	private void submitDirectoryTasks(File rootDir,
			ExecutorService managerThreadPool) throws IOException,
			FileNotFoundException {
		threadLogging("Looking in " + rootDir.getAbsolutePath());

		String[] dirNames = getSubDirectories(rootDir);

		threadLogging("Found " + dirNames.length + " directories to look in ");
		threadLogging("Max thread count for managed file import: "
				+ maxThreadCount);
		int pinsToProcess = 0;
		for (String dirName : dirNames) {
			File dir = new File(rootDir, dirName);

			if (!dir.exists()) {
				dirLogging(" + " + dirName + " DIR NOT FOUND");
			} else if (!checkForSubmissions(dir)) {
				dirLogging(" + " + dirName + " NO SUBMISSIONS");
			} else {
				dirLogging(" + " + dirName + " ADDING TO THREAD POOL");
				FileImportProcessor fip = new FileImportProcessor(dir, this);
				this.threadList.add(fip);
				managerThreadPool.execute(fip);
				pinsToProcess++;
			}
		}
		threadLogging("Found " + pinsToProcess + " directories to process");

	}

//	boolean isAlreadyProcessing(File dir) throws IOException {
//		File processingFile = new File(dir, "processing.txt");
//		boolean isProcessing = false;
//		if (processingFile.exists()) {
//			long age = System.currentTimeMillis()
//					- processingFile.lastModified();
//			if (age < 60 * 60 * 1000) {
//				// this folder might be being processed by another instance of
//				// this
//				// application
//				// this should not happen, but if it is then this process will
//				// give it
//				// at least
//				// an hour to complete. When the processing is complete the file
//				// is
//				// renamed to
//				// processed.txt
//				isProcessing = true;
//			}
//		}
//		return isProcessing;
//	}

	private boolean checkForSubmissions(File dir) {
		File submitDir = new File(dir, submitDirectoryName);
		if (!submitDir.exists()) {
			dirLogging("Submit directory does not exist, creating.");
			submitDir.mkdir();
		}
		String[] filesToProcess = submitDir.list(new FilenameFilter() {
			public boolean accept(java.io.File dir, String name) {
				File file = new File(dir, name);
				return file.isFile() && file.canRead();
			}
		});
		return filesToProcess.length > 0;
	}
	/**
	 * @return the totalFiles
	 */
	public AtomicInteger getTotalFiles() {
		return totalFiles;
	}
	/**
	 * @param totalFiles the totalFiles to set
	 */
	public void setTotalFiles(AtomicInteger totalFiles) {
		this.totalFiles = totalFiles;
	}
	/**
	 * @return the totalMessages
	 */
	public AtomicInteger getTotalMessages() {
		return totalMessages;
	}
	/**
	 * @param totalMessages the totalMessages to set
	 */
	public void setTotalMessages(AtomicInteger totalMessages) {
		this.totalMessages = totalMessages;
	}

	public void addToMessageAverage(int messages, long averageMessagetime) {
		this.avgMsgMs.set((int) (((this.avgMsgMs.get() * this.totalMessages.get()) + (messages * averageMessagetime)) / (messages + this.totalMessages.get())));
	}
	
	
}
