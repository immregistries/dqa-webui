package org.openimmunizationsoftware.dqa;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.db.model.UserAccount;
import org.openimmunizationsoftware.dqa.manager.ManagerThread;
import org.openimmunizationsoftware.dqa.manager.ManagerThreadMulti;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.manager.ReloadManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyBatchManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyExportManager;
import org.openimmunizationsoftware.dqa.quality.model.ModelFactory;

public class ConfigServlet extends HttpServlet
{

  private static final String MENU_APPLICATION = "application";
  private static final String MENU_REPORT_TEMPLATE = "reportTemplate";
  private static final String MENU_STATUS = "status";
  private static final String MENU_TEST = "test";
  private static final String MENU_LOGIN = "login";
  private static final String MENU_SETTINGS = "settings";
  private static final String MENU_RELOAD = "reload";

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
        Query query = session.createQuery("from UserAccount where username = ? and password = ?");
        query.setString(0, username);
        query.setString(1, password);
        List<UserAccount> userAccountList = query.list();
        // dqa_admin/changeme
        if (userAccountList.size() > 0)
        {
          httpSession.setAttribute("username", username);
          menu = MENU_APPLICATION;
        } else
        {
          username = null;
        }
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
      } else if (action.equals("Generate Weekly Batch"))
      {
        try
        {
          WeeklyBatchManager.getWeeklyBatchManager().runNow(sdf.parse(req.getParameter("generateDate")));
          out.println("<p class=\"pass\">Generated</p>");

        } catch (ParseException pe)
        {
          out.println("<p class=\"fail\">Unable to generate weekly batch, invalid date '" + req.getParameter("generatedDate") + "': "
              + pe.getMessage() + "</p>");
        }
      } else if (action.equals("Export Weekly Batch"))
      {
        try
        {
          WeeklyExportManager.getWeeklyExportManager().runNow(sdf.parse(req.getParameter("exportDate")));
        } catch (ParseException pe)
        {
          out.println("<p class=\"fail\">Unable to export batch, invalid date '" + req.getParameter("exportDate") + "': " + pe.getMessage() + "</p>");
          throw new ServletException(pe);
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
      out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_STATUS + "\">Status</a><br>");
      out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_SETTINGS + "\">Settings</a><br>");
      out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_RELOAD + "\">Reload</a><br>");
      out.println("      <a class=\"menuLink\" href=\"config?menu=" + MENU_TEST + "\">Test</a><br>");
      out.println("    </div>");
    }
    if (username == null || menu.equals(MENU_LOGIN))
    {
      out.println("      <h2>Login</h2>");
      out.println("    <form action=\"config\">");
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

    } else if (menu.equals(MENU_STATUS))
    {
      out.println("      <h2>Status</h2>");
      printManagerThread(IncomingServlet.fileImportManager, out);
      printManagerThread(IncomingServlet.weeklyBatchManager, out);
      printManagerThread(IncomingServlet.weeklyExportManager, out);
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
      out.println("          <input type=\"submit\" name=\"action\" value=\"update template profile\"/></td>");
      out.println("    </form>");

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

    }
    out.println("    <hr>");
    out.println("    <p>Version " + SoftwareVersion.VERSION + "</p>");
    out.println("  </body>");
    out.println("</html>");
    out.close();
    session.flush();
    session.close();
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
  }

}
