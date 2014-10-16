/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.RemoteConnection;
import org.openimmunizationsoftware.dqa.db.model.RemoteFile;
import org.openimmunizationsoftware.dqa.db.model.RemoteLog;
import org.openimmunizationsoftware.dqa.db.model.RemoteStat;
import org.openimmunizationsoftware.dqa.manager.KeyedSettingManager;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;

public class RemoteConnectionReportingServlet extends HttpServlet implements RemoteConnectionReportingInterface
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public static final long INACTIVE_TIMEOUT_WINDOW = 16 * 60 * 1000;

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (!ksm.getKeyedValueBoolean(KeyedSetting.REMOTE_ENABLED, false))
    {
      resp.sendRedirect("ui/");
      return;
    }

    PrintWriter out = new PrintWriter(resp.getOutputStream());
    try
    {
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();

      try
      {
        Transaction tx = session.beginTransaction();
        tx.begin();

        RemoteConnection remoteConnection = saveConnection(req, session);
        RemoteStat remoteStat = saveStat(req, session, remoteConnection);
        saveLog(req, session, remoteStat);
        saveFile(req, session, remoteStat);
        tx.commit();
        out.println("OK");
      } catch (Exception e)
      {
        out.println("Exception: " + e.getMessage());
        e.printStackTrace();
      } finally
      {
        session.close();
      }
    } finally
    {
      // This was added to solve an emergency problem where a client was
      // inadvertantly running a DOS attack and running
      // out bandwith.
      // synchronized (out)
      // {
      // try
      // {
      // out.wait(60 * 60 * 1000);
      // } catch (InterruptedException ie)
      // {
      // // ignore
      // }
      // }
      out.close();
    }

  }

  private RemoteConnection saveConnection(HttpServletRequest req, Session session) throws ServletException
  {
    RemoteConnection remoteConnection = null;

    RemoteConnection rcIn = new RemoteConnection();
    rcIn.setConnectionCode(readRequired(CONNECTION_CODE, req));
    rcIn.setConnectionLabel(readRequired(CONNECTION_LABEL, req));
    rcIn.setSupportCenterCode(readRequired(SUPPORT_CENTER_CODE, req));
    rcIn.setLocationTo(read(LOCATION_TO, req));
    rcIn.setLocationFrom(read(LOCATION_FROM, req));
    rcIn.setAccountName(read(ACCOUNT_NAME, req));

    Query query = session
        .createQuery("from RemoteConnection where connectionCode = ? and location_to = ? and location_from = ? and account_name = ?");
    query.setParameter(0, rcIn.getConnectionCode());
    query.setParameter(1, rcIn.getLocationTo());
    query.setParameter(2, rcIn.getLocationFrom());
    query.setParameter(3, rcIn.getAccountName());
    List<RemoteConnection> remoteConnectionList = query.list();

    if (remoteConnectionList.size() > 0)
    {
      remoteConnection = remoteConnectionList.get(0);
      remoteConnection.setConnectionLabel(rcIn.getConnectionLabel());
      remoteConnection.setSupportCenterCode(remoteConnection.getSupportCenterCode());
    } else
    {
      remoteConnection = rcIn;
    }
    session.saveOrUpdate(remoteConnection);

    return remoteConnection;
  }

  private RemoteStat saveStat(HttpServletRequest req, Session session, RemoteConnection remoteConnection) throws ServletException
  {
    RemoteStat remoteStat = null;

    Date upSinceDate = readRequiredDate(UP_SINCE_DATE, req);

    Query query = session.createQuery("from RemoteStat where remoteConnection = ? and upSinceDate = ?");
    query.setParameter(0, remoteConnection);
    query.setParameter(1, upSinceDate);
    List<RemoteStat> remoteStatList = query.list();
    if (remoteStatList.size() > 0)
    {
      remoteStat = remoteStatList.get(0);
    } else
    {
      remoteStat = new RemoteStat();
      remoteStat.setUpSinceDate(upSinceDate);
      remoteStat.setRemoteConnection(remoteConnection);
    }
    remoteStat.setStatusLabel(readRequired(STATUS_LABEL, req));
    remoteStat.setReportedDate(new Date());
    remoteStat.setAttemptCount(readRequiredInt(ATTEMPT_COUNT, req));
    remoteStat.setSentCount(readRequiredInt(SENT_COUNT, req));
    remoteStat.setErrorCount(readRequiredInt(ERROR_COUNT, req));

    session.saveOrUpdate(remoteStat);

    return remoteStat;
  }

  private void saveLog(HttpServletRequest req, Session session, RemoteStat remoteStat) throws ServletException
  {
    int pos = 0;

    String issueText = read(ISSUE_TEXT + pos, req);
    while (!issueText.equals(""))
    {
      Date reportedDate = readRequiredDate(REPORTED_DATE + pos, req);
      String exceptionTrace = read(EXCEPTION_TRACE + pos, req);
      Query query = session.createQuery("from RemoteLog where remoteStat = ? and issueText = ? and exceptionTrace = ?");
      query.setParameter(0, remoteStat);
      query.setParameter(1, issueText);
      query.setParameter(2, exceptionTrace);
      List<RemoteLog> remoteLogList = query.list();
      if (remoteLogList.size() > 0)
      {
        RemoteLog remoteLog = remoteLogList.get(0);
        remoteLog.setReportedDate(reportedDate);
        session.update(remoteLog);
      } else
      {
        RemoteLog remoteLog = new RemoteLog();
        remoteLog.setRemoteStat(remoteStat);
        remoteLog.setLogLevel(readRequiredInt(LOG_LEVEL + pos, req));
        remoteLog.setReportedDate(reportedDate);
        remoteLog.setIssueText(issueText);
        remoteLog.setExceptionTrace(read(EXCEPTION_TRACE + pos, req));
        session.save(remoteLog);
      }

      pos++;
      issueText = read(ISSUE_TEXT + pos, req);
    }
  }

  private void saveFile(HttpServletRequest req, Session session, RemoteStat remoteStat) throws ServletException
  {
    int pos = 0;

    String fileName = read(FILE_NAME + pos, req);
    while (!fileName.equals(""))
    {
      String statusLabel = readRequired(FILE_STATUS_LABEL + pos, req);
      int messageCount = readRequiredInt(FILE_MESSAGE_COUNT + pos, req);
      int sentCount = readRequiredInt(FILE_SENT_COUNT + pos, req);
      int errorCount = readRequiredInt(FILE_ERROR_COUNT + pos, req);
      Query query = session.createQuery("from RemoteFile where remoteStat = ? and fileName = ?");
      query.setParameter(0, remoteStat);
      query.setParameter(1, fileName);
      List<RemoteFile> remoteFileList = query.list();
      if (remoteFileList.size() > 0)
      {
        RemoteFile remoteFile = remoteFileList.get(0);
        if (messageCount > 0)
        {
          remoteFile.setMessageCount(messageCount);
        }
        remoteFile.setSentCount(sentCount);
        remoteFile.setErrorCount(errorCount);
        remoteFile.setStatusLabel(statusLabel);
        remoteFile.setReportedDate(new Date());
        session.update(remoteFile);
      } else
      {
        RemoteFile remoteFile = new RemoteFile();
        remoteFile.setFileName(fileName);
        remoteFile.setRemoteStat(remoteStat);
        remoteFile.setMessageCount(messageCount);
        remoteFile.setSentCount(sentCount);
        remoteFile.setErrorCount(errorCount);
        remoteFile.setStatusLabel(statusLabel);
        remoteFile.setReportedDate(new Date());
        session.save(remoteFile);
      }
      pos++;
      fileName = read(FILE_NAME + pos, req);
    }
  }

  private String readRequired(String field, HttpServletRequest req) throws ServletException
  {
    String value = req.getParameter(field);
    if (value == null || value.equals(""))
    {
      throw new ServletException("Field " + field + " is required");
    }
    return value;
  }

  private Date readRequiredDate(String field, HttpServletRequest req) throws ServletException
  {
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_TIME_FORMAT);
    String value = readRequired(field, req);
    try
    {
      return sdf.parse(value);
    } catch (ParseException pe)
    {
      throw new ServletException("Invalid value for field " + field, pe);
    }
  }

  private int readRequiredInt(String field, HttpServletRequest req) throws ServletException
  {
    String value = readRequired(field, req);
    try
    {
      return Integer.parseInt(value);
    } catch (NumberFormatException nfe)
    {
      throw new ServletException("Invalid value for field " + field, nfe);
    }
  }

  private String read(String field, HttpServletRequest req)
  {
    String value = req.getParameter(field);
    if (value == null)
    {
      return "";
    }
    return value;

  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    String action = req.getParameter("action");
    RemoteConnection remoteConnection = null;
    resp.setContentType("text/html");
    PrintWriter out = new PrintWriter(resp.getOutputStream());

    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (!ksm.getKeyedValueBoolean(KeyedSetting.REMOTE_ENABLED, false))
    {
      resp.sendRedirect("ui/");
      return;
    }
    try
    {
      out.println("<html>");
      out.println("  <head>");
      out.println("    <title>DQA Remote Connection Reporting</title>");
      out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
      out.println("  </head>");
      out.println("  <body>");
      if (action == null)
      {
        out.println("    <p>DQA remote connection reporting end point is running</p>");
        out.println("    <form action=\"remote\">");
        out.println("      <table> ");
        out.println("        <tr><td>Label</td><td><input type=\"text\" name=\"label\"/></td></tr> ");
        out.println("        <tr><td>Id</td><td><input type=\"text\" name=\"code\"/></td></tr> ");
        out.println("        <tr><td>&nbsp;</td><td><input type=\"submit\" name=\"action\" value=\"View\"/></td></tr> ");
        out.println("      </table> ");
        out.println("    </form>");
      } else if (action != null)
      {
        String label = req.getParameter("label");
        String code = req.getParameter("code");
        String statIdString = req.getParameter("statId");
        int statId = 0;
        if (statIdString != null)
        {
          statId = Integer.parseInt(statIdString);
        }
        SessionFactory factory = OrganizationManager.getSessionFactory();
        Session session = factory.openSession();
        try
        {
          Query query = session.createQuery("from RemoteConnection where connectionLabel = ? and connectionCode = ?");
          query.setParameter(0, label);
          query.setParameter(1, code);
          List<RemoteConnection> remoteConnectionList = query.list();
          if (remoteConnectionList.size() > 0)
          {
            remoteConnection = remoteConnectionList.get(0);
            out.println("<h1>Simple Message Mover</h1>");
            out.println("<h2>" + remoteConnection.getConnectionLabel() + "</h2>");
            out.println("<table>");
            out.println("  <tr><td>Location To</td><td>" + remoteConnection.getLocationTo() + "</td></tr>");
            out.println("  <tr><td>Location From</td><td>" + remoteConnection.getLocationFrom() + "</td></tr>");
            out.println("  <tr><td>Account Name</td><td>" + remoteConnection.getAccountName() + "</td></tr>");
            out.println("</table>");
            out.println("<h2>Process Reports</h2>");
            query = session.createQuery("from RemoteStat where remoteConnection = ? order by reportedDate desc");
            query.setParameter(0, remoteConnection);
            List<RemoteStat> remoteStatList = query.list();
            out.println("<table>");
            out.println("  <tr><th></th><th>Process Started</th><th>Last Reported</th><th>Active</th><th>Inactive</th><th>Attempt</th><th>Sent</th><th>Error</th></tr>");
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm a z");
            RemoteStat remoteStatSelected = null;
            code = URLEncoder.encode(code, "UTF-8");
            label = URLEncoder.encode(label, "UTF-8");
            for (RemoteStat remoteStat : remoteStatList)
            {
              boolean selected = false;
              if ((remoteStatSelected == null && statId == 0) || remoteStat.getStatId() == statId)
              {
                selected = true;
                remoteStatSelected = remoteStat;
              }

              out.println("  <tr>");
              if (selected)
              {
                out.println("    <td>&nbsp;</td>");
              } else
              {
                String link = "remote?action=View&code=" + code + "&label=" + label + "&statId=" + remoteStat.getStatId();
                out.println("    <td><a href=\"" + link + "\">Select</a></td>");
              }
              out.println("    <td>" + sdf.format(remoteStat.getUpSinceDate()) + "</td>");
              out.println("    <td>" + sdf.format(remoteStat.getReportedDate()) + "</td>");
              long elapsedTime = System.currentTimeMillis() - remoteStat.getReportedDate().getTime();
              if (elapsedTime < INACTIVE_TIMEOUT_WINDOW)
              {
                out.println("    <td>" + remoteStat.getStatusLabel() + "</td>");
                out.println("    <td>&nbsp;</td>");
              } else
              {
                out.println("    <td>&nbsp;</td>");
                out.println("    <td>" + remoteStat.getStatusLabel() + "</td>");
              }
              out.println("    <td align=\"right\">" + remoteStat.getAttemptCount() + "</td>");
              out.println("    <td align=\"right\">" + remoteStat.getSentCount() + "</td>");
              out.println("    <td align=\"right\">" + remoteStat.getErrorCount() + "</td>");
              out.println("  </tr>");
            }
            out.println("</table>");

            if (remoteStatSelected != null)
            {
              out.println("<h2>Files Processed</h2>");
              out.println("<p>Showing files processed from " + sdf.format(remoteStatSelected.getUpSinceDate()) + " to "
                  + sdf.format(remoteStatSelected.getReportedDate()) + "</p>");
              query = session.createQuery("from RemoteFile where remoteStat = ? order by reportedDate desc");
              query.setParameter(0, remoteStatSelected);
              List<RemoteFile> remoteFileList = query.list();
              out.println("<table>");
              out.println("  <tr><th>File</th><th>Reported</th><th>Status</th><th>Messages</th><th>Sent</th><th>Error</th></tr>");
              for (RemoteFile remoteFile : remoteFileList)
              {
                out.println("  <tr>");
                out.println("    <td>" + remoteFile.getFileName() + "</td>");
                out.println("    <td>" + sdf.format(remoteFile.getReportedDate()) + "</td>");
                out.println("    <td>" + remoteFile.getStatusLabel() + "</td>");
                out.println("    <td align=\"right\">" + remoteFile.getMessageCount() + "</td>");
                out.println("    <td align=\"right\">" + remoteFile.getSentCount() + "</td>");
                out.println("    <td align=\"right\">" + remoteFile.getErrorCount() + "</td>");
                out.println("  </tr>");
              }
              out.println("</table>");
              out.println("<h2>Log</h2>");
              query = session.createQuery("from RemoteLog where remoteStat = ? order by reportedDate asc");
              query.setParameter(0, remoteStatSelected);
              List<RemoteLog> remoteLogList = query.list();
              out.println("<pre>");
              for (RemoteLog remoteLog : remoteLogList)
              {
                int logLevel = remoteLog.getLogLevel();
                if (logLevel == LOG_LEVEL_ERROR)
                {
                  out.print("ERROR   ");
                } else if (logLevel == LOG_LEVEL_WARNING)
                {
                  out.print("WARNING ");
                } else if (logLevel == LOG_LEVEL_INFO)
                {
                  out.print("INFO    ");
                } else if (logLevel == LOG_LEVEL_DEBUG)
                {
                  out.print("DEBUG   ");
                }
                out.println(remoteLog.getIssueText());
                if (remoteLog.getExceptionTrace() != null && !remoteLog.getExceptionTrace().equals(""))
                {
                  out.println(remoteLog.getExceptionTrace());
                }
              }
              out.println("</pre>");
            }
          }

        } finally
        {
          session.close();
        }

      }
      out.println("  </body>");
      out.println("</html>");
    } finally
    {
      out.close();
    }
  }
}
