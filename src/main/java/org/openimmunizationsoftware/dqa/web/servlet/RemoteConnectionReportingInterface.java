package org.openimmunizationsoftware.dqa.web.servlet;

public interface RemoteConnectionReportingInterface
{
  public static final String VERSION = "1.05";
  public static final String DATE_TIME_FORMAT = "yyyyMMddHHmmssSSSZ";
  
  public static final String CONNECTION_CODE = "connectionCode";
  public static final String CONNECTION_LABEL = "connectionLabel";
  public static final String SUPPORT_CENTER_CODE = "supportCenterCode";
  public static final String LOCATION_TO = "locationTo";
  public static final String LOCATION_FROM = "locationFrom";
  public static final String ACCOUNT_NAME = "accountName";
  public static final String UP_SINCE_DATE = "upSinceDate";
  public static final String STATUS_LABEL = "statusLabel";
  public static final String ATTEMPT_COUNT = "attemptCount";
  public static final String SENT_COUNT = "sentCount";
  public static final String ERROR_COUNT = "errorCount";
  public static final String LOG_LEVEL = "logLevel";
  public static final String REPORTED_DATE = "reportedDate";
  public static final String ISSUE_TEXT = "issueText";
  public static final String EXCEPTION_TRACE = "exceptionTrace";
  public static final String FILE_NAME = "fileName";
  public static final String FILE_MESSAGE_COUNT = "fileMessageCount";
  public static final String FILE_SENT_COUNT = "fileSentCount";
  public static final String FILE_ERROR_COUNT = "fileErrorCount";
  public static final String FILE_STATUS_LABEL = "fileStatusLabel";
  
  public static final int LOG_LEVEL_ERROR = 0;
  public static final int LOG_LEVEL_WARNING = 1;
  public static final int LOG_LEVEL_INFO = 2;
  public static final int LOG_LEVEL_DEBUG = 3;
  
}
