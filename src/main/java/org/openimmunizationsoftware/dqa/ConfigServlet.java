/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.Application;
import org.openimmunizationsoftware.dqa.db.model.IssueAction;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.PotentialIssueStatus;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.Submission;
import org.openimmunizationsoftware.dqa.db.model.SubmissionAnalysis;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.db.model.UserAccount;
import org.openimmunizationsoftware.dqa.manager.DatabaseCleanupManager;
import org.openimmunizationsoftware.dqa.manager.KeyedSettingManager;
import org.openimmunizationsoftware.dqa.manager.ManagerThread;
import org.openimmunizationsoftware.dqa.manager.ManagerThreadMulti;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.manager.ReloadManager;
import org.openimmunizationsoftware.dqa.manager.UserAccountLoginManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyBatchManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyExportManager;
import org.openimmunizationsoftware.dqa.quality.model.ModelFactory;

public class ConfigServlet extends HttpServlet
{

  public static final String MENU_APPLICATION = "application";
  public static final String MENU_REPORT_TEMPLATE = "reportTemplate";
  public static final String MENU_STATUS = "status";
  public static final String MENU_TEST = "test";
  public static final String MENU_LOGIN = "login";
  public static final String MENU_SETTINGS = "settings";
  public static final String MENU_RELOAD = "reload";
  public static final String MENU_SUBMISSIONS = "submissions";
  public static final String MENU_PROFILES = "profiles";
  public static final String MENU_PROFILE = "profile";

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    doGet(req, resp);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    HttpSession httpSession = req.getSession(true);
    String menu = req.getParameter("menu");
    if (menu == null)
    {
      menu = MENU_APPLICATION;
    }
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    String username = (String) httpSession.getAttribute("username");
    PrintWriter out = new PrintWriter(resp.getOutputStream());
    resp.setContentType("text/html");

    String action = req.getParameter("action");
    if (action != null)
    {
      SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
      if (action.equals("Login"))
      {
        username = req.getParameter("username");
        String password = req.getParameter("password");
        Query query = session.createQuery("from UserAccount where username = ?");
        query.setString(0, username);
        List<UserAccount> userAccountList = query.list();
        // dqa_admin/changeme
        if (userAccountList.size() > 0)
        {
          UserAccount userAccount = userAccountList.get(0);
          boolean loggedIn = UserAccountLoginManager.login(userAccount, password, session);
          if (loggedIn)
          {
            httpSession.setAttribute("username", username);
            menu = MENU_APPLICATION;
          } else
          {
            username = null;
          }
        } else
        {
          username = null;
        }
      } else if (action.equals("logoff"))
      {
        httpSession.invalidate();
        resp.sendRedirect("config");
        return;
      } else if (action.equals("update report template"))
      {
        int templateId = Integer.parseInt(req.getParameter("templateId"));
        String reportDefinition = req.getParameter("reportDefinition");
        boolean goodToGo = false;
        // try to create model form to make sure there are no problems
        // before saving
        SubmitterProfile profile = new SubmitterProfile();
        ReportTemplate reportTemplate = new ReportTemplate();
        reportTemplate.setReportDefinition(reportDefinition);
        profile.setReportTemplate(reportTemplate);
        try
        {
          ModelFactory.createModelForm(profile);
          goodToGo = true;
        } catch (Exception e)
        {
          out.println("<p class=\"fail\">Unable to save Report Template</p>");
          out.print("<pre>");
          e.printStackTrace(out);
          out.println("</pre");
        }
        if (goodToGo)
        {
          String testCaseScript = req.getParameter("testCaseScript");
          Transaction tx = session.beginTransaction();
          reportTemplate = (ReportTemplate) session.get(ReportTemplate.class, templateId);
          reportTemplate.setReportDefinition(reportDefinition);
          reportTemplate.setTestCaseScript(testCaseScript);
          tx.commit();
          out.println("<p class=\"pass\">Report Template saved</p>");
        }
        menu = MENU_REPORT_TEMPLATE;
      } else if (action.equals("update profile"))
      {
        int templateId = Integer.parseInt(req.getParameter("templateId"));
        int profileId = Integer.parseInt(req.getParameter("profileId"));
        // try to create model form to make sure there are no problems
        // before saving
        SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, profileId);
        ReportTemplate reportTemplate = (ReportTemplate) session.get(ReportTemplate.class, templateId);

        boolean templateChanged = !reportTemplate.equals(profile.getReportTemplate());

        String profileStatus = req.getParameter("profileStatus");
        String transferPriority = req.getParameter("transferPriority");
        Transaction tx = session.beginTransaction();
        if (templateChanged)
        {
          profile.setReportTemplate(reportTemplate);
        }
        profile.setProfileStatus(profileStatus);
        profile.setTransferPriority(transferPriority);
        tx.commit();
        out.println("<p class=\"pass\">Profile saved</p>");

        menu = MENU_PROFILES;
      } else if (action.equals("Generate Weekly Batch"))
      {
        try
        {
          WeeklyBatchManager.getWeeklyBatchManager().runNow(sdf.parse(req.getParameter("generateDate")));
          out.println("<p class=\"pass\">Generated</p>");
        } catch (ParseException pe)
        {
          out.println("<p class=\"fail\">Unable to generate weekly batch, invalid date </p>");
        }
      } else if (action.equals("Cleanup Database"))
      {
        DatabaseCleanupManager.getDatabaseCleanupManager().runNow(new Date());
        out.println("<p class=\"pass\">Database Cleanup Started</p>");
      } else if (action.equals("Export Weekly Batch"))
      {
        try
        {
          WeeklyExportManager.getWeeklyExportManager().runNow(sdf.parse(req.getParameter("exportDate")));
        } catch (ParseException pe)
        {
          out.println("<p class=\"fail\">Unable to export batch, invalid date</p>");
        }
      } else if (action.equals("Reload"))
      {
        ReloadManager.triggerReload();
        out.println("<p class=\"pass\">Application has been reloaded</p>");
      } else if (action.equals("Switch"))
      {
        int applicationId = Integer.parseInt(req.getParameter("applicationId"));
        Transaction tx = session.beginTransaction();
        Query query = session.createQuery("from Application");
        List<Application> applicationList = query.list();
        for (Application application : applicationList)
        {
          application.setRunThis(applicationId == application.getApplicationId());
        }
        tx.commit();
        ReloadManager.triggerReload();
        out.println("<p class=\"pass\">Switched to new application</p>");
      } else if (action.equals("update settings"))
      {
        int applicationId = Integer.parseInt(req.getParameter("applicationId"));
        Transaction tx = session.beginTransaction();
        for (ConfigKeyedSetting configKeyedSetting : configKeyedSettingsList)
        {
          if (configKeyedSetting.keyedCode == null)
          {
            continue;
          }
          String value = req.getParameter(configKeyedSetting.keyedCode);
          if (value != null)
          {
            Query query = session.createQuery("from KeyedSetting where objectCode = ? and objectId = ? and keyedCode = ?");
            query.setString(0, "Application");
            query.setInteger(1, applicationId);
            query.setString(2, configKeyedSetting.keyedCode);
            List<KeyedSetting> keyedSettingsList = query.list();
            KeyedSetting keyedSetting = null;
            if (keyedSettingsList.size() > 0)
            {
              keyedSetting = keyedSettingsList.get(0);
              if (value == null || value.equals(""))
              {
                session.delete(keyedSetting);
              } else
              {
                keyedSetting.setKeyedValue(value);
              }
            } else if (value != null && !value.equals(""))
            {
              keyedSetting = new KeyedSetting();
              keyedSetting.setKeyedCode(configKeyedSetting.keyedCode);
              keyedSetting.setKeyedValue(value);
              keyedSetting.setObjectCode("Application");
              keyedSetting.setObjectId(applicationId);
              session.save(keyedSetting);
            }
          }
        }
        tx.commit();
        ReloadManager.triggerReload();

      }

    }

    String view = req.getParameter("view");
    if (view != null)
    {
      String submissionIdString = req.getParameter("submissionId");
      int submissionId = Integer.parseInt(submissionIdString);
      Submission submission = (Submission) session.get(Submission.class, submissionId);
      if (view.equals("report"))
      {
        printClob(out, submission.getResponseReport());
      } else if (view.equals("analysis"))
      {
        String submissionAnalysisIdString = req.getParameter("submissionAnalysisId");
        if (submissionAnalysisIdString != null)
        {
          int submissionAnalysisId = Integer.parseInt(submissionAnalysisIdString);
          SubmissionAnalysis submissionAnalysis = (SubmissionAnalysis) session.get(SubmissionAnalysis.class, submissionAnalysisId);
          printClob(out, submissionAnalysis.getAnalysisContent());
        } else
        {
          printClob(out, submission.getResponseAnalysis());
        }
      } else if (view.equals("request"))
      {
        resp.setContentType("text/plain");
        printClob(out, submission.getRequestContent());
      } else if (view.equals("response"))
      {
        resp.setContentType("text/plain");
        printClob(out, submission.getResponseContent());
      } else if (view.equals("log"))
      {
        resp.setContentType("text/plain");
        printClob(out, submission.getResponseDetailLog());
      } else if (view.equals("error"))
      {
        resp.setContentType("text/plain");
        printClob(out, submission.getResponseDetailError());
      }
    } else
    {

      out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      out.println("<html>");

      out.println("  <head>");
      out.println("    <title>DQA Config</title>");
      out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
      out.println("  </head>");
      out.println("  <body>");
      if (username != null)
      {
        out.println("    <div class=\"menu\">");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_APPLICATION + "\">Application</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_PROFILES + "\">Profiles</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_STATUS + "\">Status</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_SUBMISSIONS + "\">Submissions</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_SETTINGS + "\">Settings</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_RELOAD + "\">Reload</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_TEST + "\">Test</a><br>");
        out.println("      <a class=\"menuLink\" href=\"config?action=logoff\">Logoff</a><br>");
        out.println("    </div>");
      }
      if (username == null || menu.equals(MENU_LOGIN))
      {
        out.println("      <h2>Login</h2>");
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("      <table>");
        out.println("        <tr>");
        out.println("          <td>Username </td>");
        out.println("          <td><input type=\"text\" name=\"username\"/></td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <td>Password </td>");
        out.println("          <td><input type=\"password\" name=\"password\"/></td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <td colspan=\"2\" align=\"right\"><input type=\"submit\" name=\"action\" value=\"Login\"/></td>");
        out.println("        </tr>");
        out.println("      </table>");
        out.println("    </form>");
      } else if (menu.equals(MENU_APPLICATION))
      {
        out.println("      <h2>Applications</h2>");
        out.println("      <table>");
        out.println("        <tr>");
        out.println("          <th>Id</th>");
        out.println("          <th>Application</th>");
        out.println("          <th>Type</th>");
        out.println("          <th>Report Template</th>");
        out.println("          <th>Status</th>");
        out.println("        </tr>");
        Query query = session.createQuery("from Application order by applicationLabel, applicationType");
        List<Application> applicationList = query.list();
        for (Application application : applicationList)
        {
          out.println("        <tr>");
          out.println("          <td>" + application.getApplicationId() + "</td>");
          out.println("          <td>" + application.getApplicationLabel() + "</td>");
          out.println("          <td>" + application.getApplicationType() + "</td>");
          ReportTemplate reportTemplate = application.getPrimaryReportTemplate();
          out.println("          <td><a href=\"config?menu=" + MENU_REPORT_TEMPLATE + "&templateId=" + reportTemplate.getTemplateId() + "\">"
              + reportTemplate.getTemplateLabel() + "</a></td>");
          out.println("          <td>" + (application.getRunThis() ? "running" : "") + "</td>");
          out.println("        </tr>");
        }
        out.println("      </table>");
        out.println("    <form action=\"config\">");
        out.println("    <p>Current running application <select name=\"applicationId\">");
        for (Application application : applicationList)
        {
          out.println("<option value=\"" + application.getApplicationId() + "\"" + (application.getRunThis() ? " selected=\"true\"" : "") + ">");
          out.println(application.getApplicationLabel() + " (" + application.getApplicationType() + ")");
          out.println("</option>");
        }
        out.println("</select><input type=\"submit\" name=\"action\" value=\"Switch\"></p>");
        out.println("    </form>");

      } else if (menu.equals(MENU_PROFILES))
      {
        {
          out.println("      <h2>Submitter Profiles</h2>");
          Query query = session.createQuery("from SubmitterProfile where profileId >= 1200 order by profileCode");
          List<SubmitterProfile> profileList = query.list();
          printProfileList(out, profileList);
        }
        {
          out.println("      <h2>Base Profiles</h2>");
          Query query = session.createQuery("from SubmitterProfile where profileId < 1200 order by profileCode");
          List<SubmitterProfile> profileList = query.list();
          printProfileList(out, profileList);
        }
      } else if (menu.equals(MENU_PROFILE))
      {
        SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, Integer.parseInt(req.getParameter("profileId")));
        out.println("    <h2>Submitter Profile</h2>");
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("      <input type=\"hidden\" name=\"profileId\" value=\"" + profile.getProfileId() + "\"/>");
        out.println("      <table>");
        out.println("        <tr>");
        out.println("          <th>Profile Id</th>");
        out.println("          <td>" + profile.getProfileId() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Profile Code</th>");
        out.println("          <td>" + profile.getProfileCode() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Profile Status</th>");
        out.println("          <td>");
        out.println("            <select name=\"profileStatus\">");
        for (String profileStatus : SubmitterProfile.PROFILE_STATUS)
        {
          out.println("              <option value=\"" + profileStatus + "\""
              + (profileStatus.equals(profile.getProfileStatus()) ? " selected=\"true\"" : "") + ">" + profileStatus + "</option>");
        }
        out.println("            </select>");
        out.println("          </td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Transfer Priority</th>");
        out.println("          <td>");
        out.println("            <select name=\"transferPriority\">");
        for (String transferPriority : SubmitterProfile.TRANSFER_PRIORITY)
        {
          out.println("              <option value=\"" + transferPriority + "\""
              + (transferPriority.equals(profile.getTransferPriority()) ? " selected=\"true\"" : "") + ">" + transferPriority + "</option>");
        }
        out.println("            </select>");
        out.println("          </td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Report Template</th>");
        out.println("          <td>");
        out.println("            <select name=\"templateId\">");
        Query query = session.createQuery("from ReportTemplate order by templateLabel");
        List<ReportTemplate> reportTemplateList = query.list();
        for (ReportTemplate reportTemplate : reportTemplateList)
        {
          out.println("              <option value=\"" + reportTemplate.getTemplateId() + "\""
              + (reportTemplate.equals(profile.getReportTemplate()) ? " selected=\"true\"" : "") + ">" + reportTemplate.getTemplateLabel()
              + "</option>");
        }
        out.println("            </select>");
        out.println("          </td>");
        out.println("        <tr>");
        out.println("          <td colspan=\"2\" align=\"right\"><input type=\"submit\" name=\"action\" value=\"update profile\"/></td>");
        out.println("        </tr>");
        out.println("      </table>");
        out.println("    </form>");
      } else if (menu.equals(MENU_STATUS))
      {
        out.println("      <h2>Status</h2>");
        printManagerThread(IncomingServlet.fileImportManager, out);
        printManagerThread(IncomingServlet.submissionManager, out);
        printManagerThread(IncomingServlet.weeklyBatchManager, out);
        printManagerThread(IncomingServlet.weeklyExportManager, out);
        printManagerThread(IncomingServlet.databaseCleanupManager, out);
        out.println("    <h3>Run Database Cleanup Now</h3>");
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("    <input type=\"submit\" name=\"action\" value=\"Cleanup Database\"/>");
        out.println("    </form>");
      } else if (menu.equals(MENU_REPORT_TEMPLATE))
      {

        ReportTemplate reportTemplate = (ReportTemplate) session.get(ReportTemplate.class, Integer.parseInt(req.getParameter("templateId")));
        SubmitterProfile baseProfile = reportTemplate.getBaseProfile();
        out.println("      <h2>Report Template</h2>");
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("      <input type=\"hidden\" name=\"templateId\" value=\"" + reportTemplate.getTemplateId() + "\"/>");
        out.println("      <table>");
        out.println("        <tr>");
        out.println("          <th>Label</th>");
        out.println("          <td>" + reportTemplate.getTemplateLabel() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Id</th>");
        out.println("          <td>" + reportTemplate.getTemplateId() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Report Type</th>");
        out.println("          <td>" + reportTemplate.getReportType().getReportTypeLabel() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Base Profile</th>");
        out.println("          <td>" + baseProfile.getProfileLabel() + "</td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Report Definition</th>");
        out.println("          <td><textarea name=\"reportDefinition\" cols=\"50\" rows=\"10\">"
            + (reportTemplate.getReportDefinition() != null ? reportTemplate.getReportDefinition() : "") + "</textarea></td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <th>Test Case Script</th>");
        out.println("          <td><textarea name=\"testCaseScript\" cols=\"50\" rows=\"10\">"
            + (reportTemplate.getTestCaseScript() != null ? reportTemplate.getTestCaseScript() : "") + "</textarea></td>");
        out.println("        </tr>");
        out.println("        <tr>");
        out.println("          <td colspan=\"2\" align=\"right\"><input type=\"submit\" name=\"action\" value=\"update report template\"/></td>");
        out.println("        </tr>");
        out.println("      </table>");
        out.println("    </form>");

        Query query = session
            .createQuery("from PotentialIssueStatus where profile = ? order by issue.targetObject, issue.targetField, issue.issueType, issue.fieldValue");
        query.setParameter(0, reportTemplate.getBaseProfile());
        List<PotentialIssueStatus> potentialIssueStatusList = query.list();
        PotentialIssueStatus lastPotentialIssueStatus = null;
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("      <input type=\"hidden\" name=\"templateId\" value=\"" + reportTemplate.getTemplateId() + "\"/>");

        for (PotentialIssueStatus potentialIssueStatus : potentialIssueStatusList)
        {

          if (lastPotentialIssueStatus == null
              || !lastPotentialIssueStatus.getIssue().getTargetObject().equals(potentialIssueStatus.getIssue().getTargetObject()))

          {
            if (lastPotentialIssueStatus != null)
            {
              out.println("    </table>");
            }
            out.println("<h4>" + potentialIssueStatus.getIssue().getTargetObject() + "</h4>");
            out.println("    <table width=\"600\">");
            out.println("<tr><td width=\"40%\" bgcolor=\"#DDDDDD\">"
                + potentialIssueStatus.getIssue().getTargetObject()
                + " "
                + potentialIssueStatus.getIssue().getTargetField()
                + "</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td></tr>");
          } else if (!lastPotentialIssueStatus.getIssue().getTargetField().equals(potentialIssueStatus.getIssue().getTargetField()))
          {
            out.println("<tr><td width=\"40%\" bgcolor=\"#DDDDDD\">"
                + potentialIssueStatus.getIssue().getTargetObject()
                + " "
                + potentialIssueStatus.getIssue().getTargetField()
                + "</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td><td width=\"15%\" bgcolor=\"#DDDDDD\">&nbsp;</td></tr>");
          }
          out.println("<tr><td width=\"40%\">&nbsp;&nbsp;- " + potentialIssueStatus.getIssue().getIssueType() + " "
              + potentialIssueStatus.getIssue().getFieldValue() + "</td>");
          if (potentialIssueStatus.getAction().isError())
          {
            out.println("<td width=\"15%\" bgcolor=\"#FFFF33\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId()
                + "\" value=\"" + IssueAction.ERROR.getActionCode() + "\" checked=\"true\"/> " + potentialIssueStatus.getAction().getActionLabel()
                + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.WARN.getActionCode() + "\"/> " + IssueAction.WARN.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.SKIP.getActionCode() + "\"/> " + IssueAction.SKIP.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ACCEPT.getActionCode() + "\"/> " + IssueAction.ACCEPT.getActionLabel() + "</td>");
          } else if (potentialIssueStatus.getAction().isWarn())
          {
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ERROR.getActionCode() + "\"/> " + IssueAction.ERROR.getActionLabel() + "</td>");
            out.println("<td width=\"15%\" bgcolor=\"#FFFF33\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId()
                + "\" value=\"" + IssueAction.WARN.getActionCode() + "\" checked=\"true\"/> " + potentialIssueStatus.getAction().getActionLabel()
                + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.SKIP.getActionCode() + "\"/> " + IssueAction.SKIP.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ACCEPT.getActionCode() + "\"/> " + IssueAction.ACCEPT.getActionLabel() + "</td>");
          } else if (potentialIssueStatus.getAction().isSkip())
          {
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ERROR.getActionCode() + "\"/> " + IssueAction.ERROR.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.WARN.getActionCode() + "\"/> " + IssueAction.WARN.getActionLabel() + "</td>");
            out.println("<td width=\"15%\" bgcolor=\"#FFFF33\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId()
                + "\" value=\"" + IssueAction.SKIP.getActionCode() + "\" checked=\"true\"/> " + potentialIssueStatus.getAction().getActionLabel()
                + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ACCEPT.getActionCode() + "\"/> " + IssueAction.ACCEPT.getActionLabel() + "</td>");
          } else if (potentialIssueStatus.getAction().isAccept())
          {
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.ERROR.getActionCode() + "\"/> " + IssueAction.ERROR.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.WARN.getActionCode() + "\"/> " + IssueAction.WARN.getActionLabel() + "</td>");
            out.println("<td width=\"15%\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId() + "\" value=\""
                + IssueAction.SKIP.getActionCode() + "\"/> " + IssueAction.SKIP.getActionLabel() + "</td>");
            out.println("<td width=\"15%\" bgcolor=\"#FFFF33\"><input type=\"radio\" name=\"" + potentialIssueStatus.getPotentialIssueStatusId()
                + "\" value=\"" + IssueAction.ACCEPT.getActionCode() + "\" checked=\"true\"/> " + potentialIssueStatus.getAction().getActionLabel()
                + "</td>");
          }
          out.println("</tr>");

          lastPotentialIssueStatus = potentialIssueStatus;
        }
        out.println("    </table>");
        out.println("    <br/>");
        out.println("          <input type=\"submit\" name=\"action\" value=\"update template profile\"/></td>");
        out.println("    </form>");
        out.println("    <p><a href=\"reportTemplate?templateId=" + reportTemplate.getTemplateId() + "&action=showXML\">Report Template XML</a>");
        out.println("    </p>");

      } else if (menu.equals(MENU_TEST))
      {
        String generateDate = req.getParameter("generateDate");
        if (generateDate == null)
        {
          generateDate = "";
        }
        String exportDate = req.getParameter("exportDate");
        if (exportDate == null)
        {
          exportDate = "";
        }
        out.println("    <h2>Generate Weekly Batch</h2>");
        out.println("    <form action=\"config\" method=\"POST\">");
        out.println("    Generate Date <input type=\"text\" name=\"generateDate\" value=\"" + generateDate + "\"/><br>");
        out.println("    <input type=\"submit\" name=\"action\" value=\"Generate Weekly Batch\"/>");
        out.println("    </form>");
        if (!generateDate.equals(""))
        {
          out.println("<pre>");
          out.println(IncomingServlet.weeklyBatchManager.getInternalLog().toString());
          out.println("</pre>");
        }
        out.println("    <h2>Export Weekly Batch</h2>");
        out.println("    <form action=\"config\">");
        out.println("    Export Date <input type=\"text\" name=\"exportDate\" value=\"" + exportDate + "\"/><br>");
        out.println("    <input type=\"submit\" name=\"action\" value=\"Export Weekly Batch\"/>");
        out.println("    </form>");
        if (!exportDate.equals(""))
        {
          out.println("<pre>");
          out.println(IncomingServlet.weeklyExportManager.getInternalLog().toString());
          out.println("</pre>");
        }
      } else if (menu.equals(MENU_RELOAD))
      {
        out.println("    <h2>Reload Application Settings</h2>");
        out.println("    <p>DQA does not automatically load changes made directly the database. ");
        out.println("    Reload will ask the DQA to reread application and potential issue settings. </p>");
        out.println("    <form action=\"config\">");
        out.println("    <input type=\"submit\" name=\"action\" value=\"Reload\"/>");
        out.println("    </form>");
      } else if (menu.equals(MENU_SETTINGS))
      {
        Query query = session.createQuery("from Application where runThis = 'Y'");
        List<Application> applicationList = query.list();
        if (applicationList.size() > 0)
        {
          Application application = applicationList.get(0);
          out.println("    <h2>Settings for " + application.getApplicationLabel() + " (" + application.getApplicationType() + ")</h2>");
          out.println("    <form action=\"config\">");
          out.println("      <table>");
          out.println("      <input type=\"hidden\" name=\"applicationId\" value=\"" + application.getApplicationId() + "\"/>");

          for (ConfigKeyedSetting configKeyedSetting : configKeyedSettingsList)
          {
            query = session.createQuery("from KeyedSetting where objectCode = ? and objectId = ? and keyedCode = ?");
            query.setString(0, "Application");
            query.setInteger(1, application.getApplicationId());
            query.setString(2, configKeyedSetting.keyedCode);
            List<KeyedSetting> keyedSettingsList = query.list();
            String value = configKeyedSetting.defaultValue;
            KeyedSetting keyedSetting = null;
            if (keyedSettingsList.size() > 0)
            {
              keyedSetting = keyedSettingsList.get(0);
              value = keyedSetting.getKeyedValue();
            }

            out.println("        <tr>");
            if (!configKeyedSetting.indent)
            {
              out.println("          <th>" + configKeyedSetting.configLabel + "</th>");

            } else
            {
              out.println("          <td> &bull; " + configKeyedSetting.configLabel + "</td>");
            }
            if (configKeyedSetting.validValues != null)
            {

              out.println("          <td>");
              out.println("            <select name=\"" + configKeyedSetting.keyedCode + "\">");
              for (String option : configKeyedSetting.validValues)
              {
                out.println("              <option value=\"" + option + "\"" + (value.equals(option) ? " selected=\"true\"" : "") + ">" + option
                    + "</option>");
              }
              out.println("          </td>");
            } else
            {
              if (configKeyedSetting.keyedCode != null)
              {
                out.println("          <td><input type=\"text\" name=\"" + configKeyedSetting.keyedCode + "\" value=\"" + value + "\"</td>");
              } else
              {
                out.println("          <td>&nbsp;</td>");
              }
            }
            out.println("        <tr>");
          }
        }
        out.println("          <td colspan=\"2\" align=\"right\"><input type=\"submit\" name=\"action\" value=\"update settings\"/></td>");
        out.println("      </table>");
        out.println("    </form>");

      } else if (menu.equals(MENU_SUBMISSIONS))
      {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        String submissionIdString = req.getParameter("submissionId");
        if (submissionIdString == null)
        {
          String submitterName = req.getParameter("submitterName");
          if (submitterName == null)
          {
            Query query = session.createQuery("select submitterName from Submission group by submitterName");
            List<String> submitterNameList = query.list();
            out.println("<h2>Submissions</h2>");
            out.println("<table>");
            out.println("  <tr>");
            out.println("    <th>Submitter Name</th>");
            out.println("  </tr>");
            for (String sn : submitterNameList)
            {
              String link = "config?menu=" + MENU_SUBMISSIONS + "&submitterName=" + URLEncoder.encode(sn, "UTF-8");
              out.println("  <tr>");
              out.println("    <td><a href=\"" + link + "\">" + sn + "</a></td>");
              out.println("  </tr>");
            }
            out.println("</table>");
            submitterName = "";
          } else
          {
            Query query = session.createQuery("from Submission where submitterName = ? order by createdDate");
            query.setParameter(0, submitterName);
            List<Submission> submissionList = query.list();
            out.println("<h2>Submissions for " + submitterName + "</h2>");
            out.println("<table>");
            out.println("  <tr>");
            out.println("    <th>Id</th>");
            out.println("    <th>Submitter Name</th>");
            out.println("    <th>Request Name</th>");
            out.println("    <th>Status</th>");
            out.println("    <th>Date</th>");
            out.println("  </tr>");
            for (Submission submission : submissionList)
            {
              String link = "config?menu=" + MENU_SUBMISSIONS + "&submissionId=" + submission.getSubmissionId();
              out.println("  <tr>");
              out.println("    <td><a href=\"" + link + "\">" + submission.getSubmissionId() + "</a></td>");
              out.println("    <td><a href=\"" + link + "\">" + submission.getSubmitterName() + "</a></td>");
              out.println("    <td><a href=\"" + link + "\">" + submission.getRequestName() + "</a></td>");
              out.println("    <td><a href=\"" + link + "\">" + submission.getSubmissionStatusLabel() + "</a></td>");
              out.println("    <td><a href=\"" + link + "\">" + sdf.format(submission.getSubmissionStatusDate()) + "</a></td>");
              out.println("  </tr>");
            }
            out.println("</table>");
          }
          KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
          out.println("<h3>File Upload</h3>");
          if (ksm.getKeyedValueBoolean(KeyedSetting.UPLOAD_ENABLED, false))
          {
            out.println("    <form action=\"uploadSubmission\" enctype=\"multipart/form-data\" method=\"POST\">");
            out.println("      <table>");
            out.println("      <tr><td>Profile Name</td><td><input type=\"text\" name=\"profileCode\" value=\"" + submitterName + "\"></td>");
            out.println("      <tr><td>HL7 File</td><td><input type=\"file\" name=\"file1\"></td></tr>");
            out.println("      <tr><td colspan=\"2\" align=\"right\"><input type=\"Submit\" value=\"Upload File\"></td></tr>");
            out.println("    </table>");
            out.println("    </form>");

          } else
          {
            out.println("    <p>File upload is not enabled.</p>");
          }
        } else
        {
          int submissionId = Integer.parseInt(submissionIdString);
          Submission submission = (Submission) session.get(Submission.class, submissionId);

          String link = "config?menu=" + MENU_SUBMISSIONS + "&submissionId=" + submission.getSubmissionId();
          out.println("<h2>Submission from " + submission.getSubmitterName() + "</h2>");
          out.println("<table>");
          out.println("  <tr>");
          out.println("    <th>Id</th>");
          out.println("    <td>" + submission.getSubmissionId() + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Submitter Name</th>");
          out.println("    <td>" + submission.getSubmitterName() + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Request Name</th>");
          out.println("    <td>" + submission.getRequestName() + "</td>");
          out.println("  </tr>");
          if (submission.getProfile() != null)
          {
            out.println("  <tr>");
            out.println("    <th>Profile</th>");
            out.println("    <td>" + submission.getProfile().getProfileId() + ": " + submission.getProfile().getProfileLabel() + "</td>");
            out.println("  </tr>");
          }
          if (submission.getBatch() != null)
          {
            out.println("  <tr>");
            out.println("    <th>Message Batch</th>");
            out.println("    <td>" + submission.getBatch().getBatchId() + ": " + submission.getBatch().getBatchTitle() + "</td>");
            out.println("  </tr>");

          }
          out.println("  <tr>");
          out.println("    <th>Submission Status</th>");
          out.println("    <td>" + submission.getSubmissionStatusLabel() + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Submission Status Date</th>");
          out.println("    <td>" + sdf.format(submission.getSubmissionStatusDate()) + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Created Date</th>");
          out.println("    <td>" + sdf.format(submission.getCreatedDate()) + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Submitter Defined Value 1</th>");
          out.println("    <td>" + submission.getSubmitterDefinedValue1() + "</td>");
          out.println("  </tr>");
          out.println("  <tr>");
          out.println("    <th>Submitter Defined Value 2</th>");
          out.println("    <td>" + submission.getSubmitterDefinedValue2() + "</td>");
          out.println("  </tr>");
          out.println("</table>");
          out.println("<h3>Request</h3>");
          printClobLimited(out, submission.getRequestContent());
          if (submission.getResponseContent() != null)
          {
            out.println("<h3>Response</h3>");
            printClobLimited(out, submission.getResponseContent());
          }
          if (submission.getResponseDetailLog() != null)
          {
            out.println("<h3>Log</h3>");
            printClobLimited(out, submission.getResponseDetailLog());
          }
          if (submission.getResponseDetailError() != null)
          {
            out.println("<h3>Error Log</h3>");
            printClobLimited(out, submission.getResponseDetailError());
          }
          if (submission.getResponseReport() != null)
          {
            out.println("<h3>DQA Report</h3>");
            out.println("<p><a href=\"" + link + "&view=report\" target=\"_blank\">View</a></p>");
          }
          if (submission.getResponseAnalysis() != null)
          {
            out.println("<h3>Analysis</h3>");
            out.println("<p><a href=\"" + link + "&view=analysis\" target=\"_blank\">View</a></p>");
            Query query = session.createQuery("from SubmissionAnalysis where submission = ?");
            query.setParameter(0, submission);
            List<SubmissionAnalysis> submissionAnalsyisList = query.list();
            if (submissionAnalsyisList.size() > 0)
            {
              out.println("<table>");
              out.println("  <tr>");
              out.println("    <th>Analysis Label</th>");
              out.println("    <th>Message Received Id</th>");
              out.println("  <tr>");
              for (SubmissionAnalysis submissionAnalysis : submissionAnalsyisList)
              {
                String analysisLink = link + "&view=analysis&submissionAnalysisId=" + submissionAnalysis.getSubmissionAnalysisId();
                out.println("  <tr>");
                out.println("    <td><a href=\"" + analysisLink + "\" target=\"_blank\">" + submissionAnalysis.getAnalysisLabel() + "</a></td>");
                out.println("    <td><a href=\"" + analysisLink + "\" target=\"_blank\">" + submissionAnalysis.getMessageReceived().getReceivedId()
                    + "</a></td>");
                out.println("  <tr>");
              }
              out.println("</table>");
            }
          }

        }
      }
      out.println("    <hr>");
      out.println("    <p>Version " + SoftwareVersion.VERSION + "</p>");
      out.println("  </body>");
      out.println("</html>");
    }
    out.close();
    session.flush();
    session.close();
  }

  public void printProfileList(PrintWriter out, List<SubmitterProfile> profileList)
  {
    out.println("      <table>");
    out.println("        <tr>");
    out.println("          <th>Id</th>");
    out.println("          <th>Code</th>");
    out.println("          <th>Status</th>");
    out.println("          <th>Report Template</th>");
    out.println("        </tr>");
    for (SubmitterProfile profile : profileList)
    {
      out.println("        <tr>");
      out.println("          <td>" + profile.getProfileId() + "</td>");
      out.println("          <td><a href=\"config?menu=" + MENU_PROFILE + "&profileId=" + profile.getProfileId() + "\">" + profile.getProfileCode()
          + "</a></td>");
      out.println("          <td>" + profile.getProfileStatus() + "</td>");
      ReportTemplate reportTemplate = profile.getReportTemplate();
      out.println("          <td><a href=\"config?menu=" + MENU_REPORT_TEMPLATE + "&templateId=" + reportTemplate.getTemplateId() + "\">"
          + reportTemplate.getTemplateLabel() + "</a></td>");
      out.println("        </tr>");
    }
    out.println("      </table>");
  }

  public void printClobLimited(PrintWriter out, Clob requestContent) throws IOException
  {
    out.println("<pre>");
    try
    {
      BufferedReader reader;
      reader = new BufferedReader(requestContent.getCharacterStream());
      int count = 0;
      String line;
      while (count < 100 && (line = reader.readLine()) != null)
      {
        count++;
        out.println(line);
      }
      if (count == 100)
      {
        out.println("... [more content, but it is not displayed here]");
      }
      reader.close();
    } catch (SQLException sqle)
    {
      sqle.printStackTrace(out);
    }
    out.println("</pre>");
  }

  public void printClob(PrintWriter out, Clob requestContent) throws IOException
  {
    try
    {
      BufferedReader reader;
      reader = new BufferedReader(requestContent.getCharacterStream());
      String line;
      while ((line = reader.readLine()) != null)
      {
        out.println(line);
      }
      reader.close();
    } catch (SQLException sqle)
    {
      sqle.printStackTrace(out);
    }
  }

  public void printClobForAnalysis(PrintWriter out, Clob requestContent, int submissionId) throws IOException
  {
    try
    {
      BufferedReader reader;
      reader = new BufferedReader(requestContent.getCharacterStream());
      String line;
      int pos = 0;
      while ((line = reader.readLine()) != null)
      {
        if ((pos = line.indexOf("href=\"Message 1")) > -1)
        {
          pos = pos + 6;
          out.print(line.substring(0, pos));
          out.print("menu=" + MENU_SUBMISSIONS + "&submissionId=" + submissionId + "&submissionAnalysisId");
        }
        out.println(line);
      }
      reader.close();
    } catch (SQLException sqle)
    {
      sqle.printStackTrace(out);
    }
  }

  private void printManagerThread(ManagerThread mt, PrintWriter out)
  {
    if (mt == null)
    {
      out.println("<p>Manager thread is not defined.</p>");
      return;
    }
    if (mt.isKeepRunning())
    {
      out.println("      <h3>Process Deamon '" + mt.getLabel() + "' is running.</h3>");
    }
    if (mt.getLastException() != null)
    {
      out.println("      <h4>Last Exception</h4>");
      out.println("<pre>");
      mt.getLastException().printStackTrace(out);
      out.println("</pre>");
    }
    out.println("      <h4>Internal Log</h4>");
    out.println("<pre>");
    out.print(mt.getInternalLog());
    out.println("</pre>");
    out.println("      <h4>Progress Update</h4>");
    if (mt.getProgressStart() > 0)
    {
      out.println("<pre>");
      SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
      out.println("  + Started: " + sdf.format(mt.getProgressStart()));
      out.println("  + Count:   " + mt.getProgressCount());
      out.println("  + Rate:    " + ((float) mt.getProgressCount()) / ((System.currentTimeMillis() - mt.getProgressStart()) / 1000.0));
      out.println("</pre>");
    }
    if (mt instanceof ManagerThreadMulti)
    {
      ManagerThreadMulti mtm = (ManagerThreadMulti) mt;
      for (ManagerThread mtChild : mtm.getManagerThreads())
      {
        printManagerThread(mtChild, out);
      }
    }
  }

  private static class ConfigKeyedSetting
  {
    private String keyedCode = "";
    private String configLabel = "";
    // private String configDescription = "";
    private boolean indent = false;
    private String[] validValues = null;
    private String defaultValue = "";

    private ConfigKeyedSetting(String keyedCode, String configLabel, String configDescription) {
      this.keyedCode = keyedCode;
      this.configLabel = configLabel;
      // this.configDescription = configDescription;
    }

    public ConfigKeyedSetting setIndent()
    {
      indent = true;
      return this;
    }

    public ConfigKeyedSetting setValidValues(String[] validValues)
    {
      this.validValues = validValues;
      return this;
    }

    public ConfigKeyedSetting setDefaultValue(String defaultValue)
    {
      this.defaultValue = defaultValue;
      return this;
    }
  }

  private static List<ConfigKeyedSetting> configKeyedSettingsList = new ArrayList<ConfigServlet.ConfigKeyedSetting>();
  static
  {
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.APPLICATION_EXTERNAL_URL_BASE, "Application external URL base", ""));

    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_ENABLE, "Read in file enabled", "").setValidValues(new String[] { "",
        "Y", "N" }));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_DIR, "Base directory path", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_ACCEPTED_DIR_NAME, "Accepted directory name", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_DQA_DIR_NAME, "DQA directory name", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_RECEIVE_DIR_NAME, "Receive directory name", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_SUBMIT_DIR_NAME, "Submit directory name", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_THREAD_COUNT_MAX, "Processing thread count", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_WAIT, "Wait after last update (secs)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_FILE_EXPORT_CONNECTION_SCRIPT, "Export connection script", "").setValidValues(
        new String[] { "", "Y", "N" }).setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_SUBMISSION_ENABLE, "Read submission table enabled", "")
        .setValidValues(new String[] { "", "Y", "N" }));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.IN_SUBMISSION_WAIT, "Pause after checking for updates (secs)", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.UPLOAD_ENABLED, "Upload enabled", "")
        .setValidValues(new String[] { "", "Y", "N" }));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.UPLOAD_DIR, "Uploaded local file directory name", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Export batches enabled", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_FILE_DIR, "Base directory path", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_HL7_MSH_PROCESSING_ID, "MSH Processing Id", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_HL7_MSH_RECEIVING_APPLICATION, "MSH Receiving Application", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_HL7_MSH_RECEIVING_FACILITY, "MSH Receiving Facility", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_HL7_MSH_SENDING_APPLICATION, "MSH Sending Application", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.OUT_HL7_MSH_VERSION_ID, "MSH Version Id", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Validate header", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_HEADER_SENDING_FACILITY_MAX_LEN, "Sending facility max length", "")
        .setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_HEADER_SENDING_FACILITY_MIN_LEN, "Sending facility min length", "")
        .setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_HEADER_SENDING_FACILITY_NUMERIC, "Sending facility is numeric", "")
        .setValidValues(new String[] { "", "Y", "N" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_HEADER_SENDING_FACILITY_PFS, "Sending facility is PFS", "")
        .setValidValues(new String[] { "", "Y", "N" }).setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Validate patient", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_PATIENT_FINANCIAL_STATUS_IGNORE, "Deprecated VFC status (PV1-20)", "")
        .setValidValues(new String[] { "", "Y", "N" }));

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Validate vaccination", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_VACCINATION_FACILITY_MAX_LEN, "Vaccination facility max length", "")
        .setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_VACCINATION_FACILITY_MIN_LEN, "Vaccination facility min length", "")
        .setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_VACCINATION_FACILITY_NUMERIC, "Vaccination facility is numeric", "")
        .setValidValues(new String[] { "", "Y", "N" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.VALIDATE_VACCINATION_FACILITY_PFS, "Vaccination facility is PFS", "")
        .setValidValues(new String[] { "", "Y", "N" }).setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "DQA report", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DQA_REPORT_READY_FOR_PRODUCTION_ENABLED, "Ready for Production flag enabled", "")
        .setValidValues(new String[] { "", "Y", "N" }).setIndent().setDefaultValue("Y"));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DQA_REPORT_READY_FOR_PRODUCTION_TRIGGER_LEVEL,
        "Ready for Production trigger level", "").setIndent().setDefaultValue("50"));

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Weekly batch", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_BATCH_DAY, "Batch day (1=Sunday)", "").setValidValues(
        new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_BATCH_START_TIME, "Batch after (HH:MM)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_BATCH_END_TIME, "Batch before (HH:MM)", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Weekly export", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_DAY_HIGHEST, "Export day for highest priority (2=Monday)", "")
        .setValidValues(new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_DAY_HIGH, "Export day for high priority (2=Monday)", "")
        .setValidValues(new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_DAY_NORMAL, "Export day for normal priority (2=Monday)", "")
        .setValidValues(new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_DAY_LOW, "Export day for low priority (2=Monday)", "")
        .setValidValues(new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_DAY_LOWEST, "Export day for lowest priority (2=Monday)", "")
        .setValidValues(new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_START_TIME, "Export time after (HH:MM)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.WEEKLY_EXPORT_END_TIME, "Export time before (HH:MM)", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Database Cleanup", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_ENABLED, "Database Cleanup Enabled", "").setValidValues(
        new String[] { "", "Y", "N" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_DAY, "Cleanup day (1=Sunday)", "").setValidValues(
        new String[] { "", "1", "2", "3", "4", "5", "6", "7" }).setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_START_TIME, "Cleanup after (HH:MM)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_END_TIME, "Cleanup before (HH:MM)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_DATA_FIELDS_AFTER_DAYS,
        "Delete Data Fields received after (in days)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_MESSAGE_TEXT_AFTER_DAYS,
        "Delete Message Text received after (in days)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_MESSAGE_ANALYSIS_AFTER_DAYS,
        "Delete Message Analysis created after (in days)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_BATCH_REPORTS_SUBMITTED_AFTER_DAYS,
        "Delete Submitted Batch Reports created after (in days)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_BATCH_REPORTS_WEEKLY_AFTER_DAYS,
        "Delete Weekly Batch Reports created after (in days)", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.DATABASE_CLEANUP_SUBMISSIONS_AFTER_DAYS,
        "Delete Submissions last updated after (in days)", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "CDS Software", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.CDS_SOFTWARE_SERVICE_ENABLED, "Service enabled", "").setIndent().setValidValues(
        new String[] { "", "Y", "N" }));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.CDS_SOFTWARE_SERVICE_TYPE, "Service type", "").setIndent());
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.CDS_SOFTWARE_SERVICE_URL, "Service URL", "").setIndent());

    configKeyedSettingsList.add(new ConfigKeyedSetting(null, "Remote Reporting", ""));
    configKeyedSettingsList.add(new ConfigKeyedSetting(KeyedSetting.REMOTE_ENABLED, "Remote Servlet Enabled", "").setValidValues(
        new String[] { "", "Y", "N" }).setIndent());

  }

}
