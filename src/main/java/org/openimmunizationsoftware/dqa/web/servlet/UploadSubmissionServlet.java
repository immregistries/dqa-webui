/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web.servlet;

import org.apache.wicket.util.file.FileDeleteStrategy;
import org.apache.wicket.util.file.IFileCleaner;
import org.apache.wicket.util.upload.DiskFileItemFactory;
import org.apache.wicket.util.upload.FileItem;
import org.apache.wicket.util.upload.ServletFileUpload;
import org.hibernate.*;
import org.openimmunizationsoftware.dqa.ConfigServlet;
import org.openimmunizationsoftware.dqa.db.model.*;
import org.openimmunizationsoftware.dqa.manager.KeyedSettingManager;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.*;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

public class UploadSubmissionServlet extends HttpServlet
{

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    HttpSession httpSession = req.getSession(true);

    String username = (String) httpSession.getAttribute("username");

    if (username == null)
    {
      RequestDispatcher requestDispatcher = req.getRequestDispatcher("config");
      requestDispatcher.forward(req, resp);
      return;
    }

    String action = req.getParameter("action");
    if (action == null)
    {

      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();

      resp.setContentType("text/html");
      PrintWriter out = new PrintWriter(resp.getOutputStream());
      out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      out.println("<html>");
      out.println("  <head>");
      out.println("    <title>DQA Upload Submission</title>");
      out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
      out.println("  </head>");
      out.println("  <body>");
      out.println("    <h1>Upload DQA File</h1>");
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      if (ksm.getKeyedValueBoolean(KeyedSetting.UPLOAD_ENABLED, false))
      {
        String profileCode = req.getParameter("profileCode");
        if (profileCode == null)
        {
          profileCode = "";
        }
        out.println("    <form action=\"uploadSubmission\" enctype=\"multipart/form-data\" method=\"POST\">");
        out.println("      <table>");
        out.println("      <tr><td>Profile Name</td><td><input type=\"text\" name=\"profileCode\" value=\"" + profileCode + "\"></td>");

        out.println("      <tr><td>HL7 File</td><td><input type=\"file\" name=\"file1\"></td></tr>");
        out.println("      <tr><td colspan=\"2\" align=\"right\"><input type=\"Submit\" value=\"Upload File\"></td></tr>");
        out.println("    </table>");
        out.println("    </form>");
      } else
      {
        out.println("    <p>File upload is not enabled.</p>");
      }
      out.println("  </body>");
      out.println("</html>");

      out.close();
      session.close();
    } else if (action.equals("ack") || action.equals("log") || action.equals("errors"))
    {
      File file = (File) httpSession.getAttribute(action);
      resp.setContentType("text/plain");
      resp.setHeader("Content-Disposition", "Attachment;filename=\"" + file.getName() + "\"");
      PrintWriter out = new PrintWriter(resp.getOutputStream());
      String line;
      BufferedReader in = new BufferedReader(new FileReader(file));
      while ((line = in.readLine()) != null)
      {
        out.println(line);
      }
      in.close();
      out.close();
    } else if (action.equals("report"))
    {
      File ackFile = (File) httpSession.getAttribute(action);
      resp.setContentType("text/html");
      PrintWriter out = new PrintWriter(resp.getOutputStream());
      String line;
      BufferedReader in = new BufferedReader(new FileReader(ackFile));
      while ((line = in.readLine()) != null)
      {
        out.println(line);
      }
      in.close();
      out.close();
    }
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    HttpSession httpSession = req.getSession(true);
    String username = (String) httpSession.getAttribute("username");

    if (username == null)
    {
      RequestDispatcher requestDispatcher = req.getRequestDispatcher("config");
      requestDispatcher.forward(req, resp);
      return;
    }
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (ksm.getKeyedValueBoolean(KeyedSetting.UPLOAD_ENABLED, false))
    {
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();
      PrintWriter out = resp.getWriter();
      resp.setContentType("text/html");
      out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      out.println("<html>");
      out.println("  <head>");
      out.println("    <title>DQA Upload Submission</title>");
      out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
      out.println("  </head>");
      out.println("  <body>");
      out.println("    <h1>Upload DQA File</h1>");

      IFileCleaner fileCleaner = new IFileCleaner() {

        public void track(File file, Object marker, FileDeleteStrategy deleteStrategy)
        {
          // TODO Auto-generated method stub

        }

        public void track(File file, Object marker)
        {
          // TODO Auto-generated method stub

        }

        public void destroy()
        {
          // TODO Auto-generated method stub

        }
      };

      DiskFileItemFactory fileItemFactory = new DiskFileItemFactory(fileCleaner);
      fileItemFactory.setSizeThreshold(1 * 1024 * 1024); // 1 MB
      String uploadDirString = ksm.getKeyedValue(KeyedSetting.UPLOAD_DIR, ".");

      // TODO:   For server deploy Uncomment next line; it is set to "//srv/tomcatc/appdata/dqa/" in dqa_keyed_setting table.
      File uploadDir = new File(uploadDirString);

        // TODO:  For server deploy Comment out next line;
        // //For local debugging only: set C:\MDHApps\dataupload folder for uploadDirString
        //File uploadDir = new File("C:\\MDHApps\\dataupload"); // ???


      if (!uploadDir.exists())
      {
        throw new IllegalArgumentException("Upload directory not found, unable to upload");
      }
      File uploadDirTemp = new File(uploadDir, "/temp");
      if (!uploadDirTemp.exists())
      {
        uploadDirTemp.mkdir();
      }
      File uploadDirDest = new File(uploadDirTemp, "/process");
      if (!uploadDirDest.exists())
      {
        uploadDirDest.mkdir();
      }
      String profileCode = "";
      fileItemFactory.setRepository(uploadDirTemp);
      ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
      File file = null;
      String filename = "";
      try
      {
        List<FileItem> items = uploadHandler.parseRequest(req);
        for (FileItem item : items)
        {
          /*
           * Handle Form Fields.
           */
          if (item.isFormField())
          {
            if (item.getFieldName().equals("profileCode"))
            {
              profileCode = item.getString();
            }
          } else
          {
            file = new File(uploadDirDest, item.getName());
              //file = new File(uploadDirDest, "testdqa.txt"); // testdqa.txt   for local testing ??
              StringTokenizer tokenizer = new StringTokenizer("\\");
              StringTokenizer st2 = new StringTokenizer(item.getName());
              String temp = "";
              while(st2.hasMoreTokens()) {
                  //System.out.println(st2.nextToken("\\"));
                  temp = "";
                  temp = st2.nextToken("\\");
                  //System.out.println(temp);

              }

              file = new File(uploadDirDest, temp);

              // Works in DEV but show the whole path.
              //filename = item.getName();

              // Locally next line works but in DEV causes issue.
              filename = temp; // just the file name not the whole path/fileName

            item.write(file);
          }
        }
      } catch (Exception ex)
      {
        throw new ServletException("Unable to upload file", ex);
      }

      Transaction trans = session.beginTransaction();
      Submission submission = new Submission();
      submission.setSubmitterName(profileCode);
      submission.setRequestName(filename);
      submission.setReturnResponse(true);
      submission.setReturnDetailLog(true);
      submission.setReturnDetailError(true);
      submission.setReturnReport(true);
      submission.setReturnAnalysis(true);
      submission.setSubmissionStatus(Submission.SUBMISSION_STATUS_CREATED);
      submission.setSubmissionStatusDate(new Date());
      submission.setCreatedDate(new Date());
      session.save(submission);
      session.flush();
      FileReader fileReader = new FileReader(file);
      submission.setRequestContent(Hibernate.createClob(fileReader, file.length(), session));
      submission.setSubmissionStatus(Submission.SUBMISSION_STATUS_SUBMITTED);
      submission.setSubmissionStatusDate(new Date());
      session.update(submission);
      session.flush();
      trans.commit();

        // TODO: This set dqa_submission.submission_status to E always.       ????
      out.println("   <p>File uploaded for processing.</p>");
      out.println("   <p><a href=\"config?menu=" + ConfigServlet.MENU_SUBMISSIONS + "&submitterName=" + URLEncoder.encode(profileCode, "UTF-8")
          + "\">View Submissions</a></p>");

      out.println("  </body>");
      out.println("</html>");
      out.close();
      session.close();
      return;
    }
    doGet(req, resp);

  }

  private SubmitterProfile findProfile(String profileCode, int templateId, Session session)
  {
    SubmitterProfile profile = null;
    // Looking for submitter profile
    Query query = session.createQuery("from SubmitterProfile where profileCode = ?");
    query.setParameter(0, profileCode);
    List<SubmitterProfile> submitterProfiles = query.list();
    if (submitterProfiles.size() == 0)
    {
      // Submitter profile not found, creating new one
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
      profile.setReportTemplate((ReportTemplate) session.get(ReportTemplate.class, templateId));
      session.save(organization);
      session.save(profile);
      organization.setPrimaryProfile(profile);
      tx.commit();
    } else
    {
      profile = submitterProfiles.get(0);
    }
    profile.getOrganization().getOrgLabel(); // did this to force lazy loading
                                             // now
    return profile;
  }

}
