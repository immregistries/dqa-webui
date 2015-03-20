package org.openimmunizationsoftware.dqa;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openimmunizationsoftware.dqa.db.model.IssueAction;
import org.openimmunizationsoftware.dqa.db.model.PotentialIssue;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.manager.PotentialIssues;

public class IssueDocumentationServlet extends HttpServlet
{
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    PrintWriter out = new PrintWriter(resp.getOutputStream());
    resp.setContentType("text/html");

    PotentialIssues potentialIssues = PotentialIssues.getPotentialIssues();

    String idString = req.getParameter("id");
    // DQA0257
    out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
    out.println("<html>");
    out.println("  <head>");
    out.println("    <title>DQA Issue</title>");
    out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
    out.println("  </head>");
    out.println("  <body>");
    boolean found = false;
    if (idString != null && idString.startsWith("DQA"))
    {
      IssueAction issueAction;
      String severity = req.getParameter("s");
      if (severity == null || severity.equals(""))
      {
        issueAction = IssueAction.WARN;
      } else if (severity.equals("E"))
      {
        issueAction = IssueAction.ERROR;
      } else if (severity.equals("W"))
      {
        issueAction = IssueAction.WARN;
      } else if (severity.equals("I"))
      {
        issueAction = IssueAction.ACCEPT;
      } else
      {
        issueAction = IssueAction.WARN;
      }
      int issueId = Integer.parseInt(idString.substring(3));
      PotentialIssue potentialIssue = (PotentialIssue) session.get(PotentialIssue.class, issueId);
      if (potentialIssue != null)
      {
        found = true;
        out.print(potentialIssues.getDocumentationForAnalysis(potentialIssue, issueAction));
      }
    }
    if (!found)
    {
      out.println("  <p>Unrecognized issue: <b>" + idString + "</b></p>");
    }

    out.println("  <h3>Other Issues</h3>");
    out.println("  <table>");
    out.println("    <tr><th>Issue</th><th>HL7 Ref</th><th>Description</th></tr>");
    for (PotentialIssue issue : potentialIssues.getAllPotentialIssues())
    {
      out.println("    <tr>");
      out.println("      <td><a href=\"issue?id=DQA" + issue.getIssueId() + "\">" + issue.getDisplayText() + "</a></td>");
      if (issue.getHl7Reference() == null)
      {
        out.println("      <td>-</td>");
      } else
      {
        out.println("      <td>" + issue.getHl7Reference() + "</td>");
      }
      out.println("      <td>" + issue.getIssueDescription() + "</td>");
      out.println("    </tr>");
    }
    out.println("    </table>");
    out.println("  </body>");
    out.println("</html>");

    out.close();
    session.flush();
    session.close();

  }
}
