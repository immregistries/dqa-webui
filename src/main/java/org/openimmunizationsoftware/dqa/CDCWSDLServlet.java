/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openimmunizationsoftware.dqa.manager.DatabaseCleanupManager;
import org.openimmunizationsoftware.dqa.manager.FileImportManager;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.manager.SubmissionManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyBatchManager;
import org.openimmunizationsoftware.dqa.manager.WeeklyExportManager;
import org.openimmunizationsoftware.dqa.transport.CDCWSDLServer;
import org.openimmunizationsoftware.dqa.transport.SubmitSingleMessage;

public class CDCWSDLServlet extends IncomingServlet
{

  protected static FileImportManager fileImportManager = null;
  protected static SubmissionManager submissionManager = null;
  protected static WeeklyBatchManager weeklyBatchManager = null;
  protected static WeeklyExportManager weeklyExportManager = null;
  protected static DatabaseCleanupManager databaseCleanupManager = null;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    resp.setContentType("text/xml");
    PrintWriter out = new PrintWriter(resp.getOutputStream());
    out.println("<html>");
    out.println("  <head>");
    out.println("    <title>DQA Incoming Interface</title>");
    out.println("  </head>");
    out.println("  <body>");
    out.println("    <h1>CDC WSDL Endpoint</h1>");
    out.println("    <hr>");
    out.println("    <p>Version " + SoftwareVersion.VERSION + " released " + SoftwareVersion.RELEASE_DATE + "</p>");
    out.println("  </body>");
    out.println("</html>");
    out.close();
    out = null;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    final Session session = factory.openSession();
    try
    {
      CDCWSDLServer server = new CDCWSDLServer() {
        @Override
        public void process(SubmitSingleMessage ssm, PrintWriter out) throws IOException
        {
          IncomingServlet.processMessage(ssm, out, session);
        }

        @Override
        public String getEchoBackMessage(String message)
        {
          return "DQA CDC WSDL is operational. Received: " + message + "";
        }

        @Override
        public void authorize(SubmitSingleMessage ssm)
        {
          IncomingServlet.authorize(ssm, session);
        }
      };
      server.process(req, resp);
    } finally
    {
      session.flush();
      session.close();
    }
  }

}
