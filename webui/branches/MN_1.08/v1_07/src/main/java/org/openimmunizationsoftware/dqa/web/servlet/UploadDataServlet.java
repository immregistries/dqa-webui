/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.wicket.util.file.FileDeleteStrategy;
import org.apache.wicket.util.file.IFileCleaner;
import org.apache.wicket.util.upload.DiskFileItemFactory;
import org.apache.wicket.util.upload.FileItem;
import org.apache.wicket.util.upload.ServletFileUpload;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.ProcessLocker;
import org.openimmunizationsoftware.dqa.db.model.KeyedSetting;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.ReportTemplate;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.FileImportProcessorCore;
import org.openimmunizationsoftware.dqa.manager.KeyedSettingManager;
import org.openimmunizationsoftware.dqa.manager.ManagerThread;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.parse.VaccinationParserHL7;

public class UploadDataServlet extends HttpServlet
{

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    HttpSession httpSession = req.getSession(true);
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
      out.println("    <title>DQA Upload</title>");
      out.println("    <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />");
      out.println("  </head>");
      out.println("  <body>");
      out.println("    <h1>Upload DQA File</h1>");
      KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
      if (ksm.getKeyedValueBoolean(KeyedSetting.UPLOAD_ENABLED, false))
      {
        ReportTemplate reportTemplate = null;
        String profileCode = req.getParameter("profileCode");
        if (profileCode == null)
        {
          profileCode = "";
        } else if (!profileCode.equals(""))
        {
          Query query = session.createQuery("from SubmitterProfile where profileCode = ?");
          List<SubmitterProfile> submitterProfileList = query.list();
          if (submitterProfileList.size() > 0)
          {
            reportTemplate = submitterProfileList.get(0).getReportTemplate();
          }
        }
        out.println("    <form action=\"uploadData\" enctype=\"multipart/form-data\" method=\"POST\">");
        out.println("      <table>");
        out.println("      <tr><td>Profile Name</td><td><input type=\"text\" name=\"profileCode\" value=\"" + profileCode + "\"></td>");
        out.println("      <tr><td>Template</td><td>");
        if (reportTemplate == null)
        {
          Query query = session.createQuery("from ReportTemplate order by templateLabel");
          List<ReportTemplate> reportTemplateList = query.list();
          out.println("      <select name=\"templateId\">");
          for (ReportTemplate reportTemplateSelected : reportTemplateList)
          {
            out.println("        <option value=\"" + reportTemplateSelected.getTemplateId() + "\">" + reportTemplateSelected.getTemplateLabel()
                + "</option>");
          }
          out.println("      </select>");
        } else
        {
          out.println(reportTemplate.getTemplateLabel());
        }
        out.println("</td></tr>");
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

  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    KeyedSettingManager ksm = KeyedSettingManager.getKeyedSettingManager();
    if (ksm.getKeyedValueBoolean(KeyedSetting.UPLOAD_ENABLED, false))
    {
      SessionFactory factory = OrganizationManager.getSessionFactory();
      Session session = factory.openSession();
      PrintWriter out = response.getWriter();
      response.setContentType("text/html");
      out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      out.println("<html>");
      out.println("  <head>");
      out.println("    <title>DQA Upload</title>");
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
      File uploadDir = new File(uploadDirString);
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
      int templateId = 0;
      fileItemFactory.setRepository(uploadDirTemp);
      ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
      File file = null;
      String filename = "";
      try
      {
        List<FileItem> items = uploadHandler.parseRequest(request);
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
            } else if (item.getFieldName().equals("templateId"))
            {
              templateId = Integer.parseInt(item.getString());
            }
          } else
          {
            file = new File(uploadDirDest, item.getName());
            filename = item.getName();
            item.write(file);
          }
        }
      } catch (Exception ex)
      {
        throw new ServletException("Unable to upload file", ex);
      }
      SubmitterProfile profile = findProfile(profileCode, templateId, session);
      if (profile.isProfileStatusTest())
      {
        VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
        Transaction tx = session.beginTransaction();
        profile.initPotentialIssueStatus(session);
        tx.commit();
        
        try
        {
          ProcessLocker.lock(profile);
          File acceptedDir = new File(uploadDir, "/" + profile.getProfileCode());
          if (!acceptedDir.exists())
          {
            acceptedDir.mkdir();
          }
          ManagerThread thread = new ManagerThread("UploadDataServlet");
          out.println("<p>Processing log:</p>");
          out.println("<pre class=\"scrollbox\">");
          FileImportProcessorCore fileImportProcessorCore = new FileImportProcessorCore(out, thread, profile, parser, acceptedDir, acceptedDir);
          fileImportProcessorCore.processFile(session, filename, file);
          out.println("</pre>");
          HttpSession httpSession = request.getSession(true);
          httpSession.setAttribute("ack", fileImportProcessorCore.getAckFile());
          httpSession.setAttribute("log", fileImportProcessorCore.getLogFile());
          httpSession.setAttribute("errors", fileImportProcessorCore.getErrorsFile());
          httpSession.setAttribute("report", fileImportProcessorCore.getReportFile());
          out.println("<p>Data was loaded. Records available:</p>");
          out.println("<ul>");
          out.println("<li><a href=\"" + response.encodeUrl("uploadData?action=ack") + "\">Acknowledgements File</a></li>");
          out.println("<li><a href=\"" + response.encodeUrl("uploadData?action=log") + "\">Log File</a></li>");
          out.println("<li><a href=\"" + response.encodeUrl("uploadData?action=errors") + "\">Errors File</a></li>");
          out.println("<li><a href=\"" + response.encodeUrl("uploadData?action=report") + "\">DQA Report</a></li>");
          out.println("</ul>");
        } finally
        {
          ProcessLocker.unlock(profile);
        }
      } else
      {
        out.println("<p>Profile is not in Test. Unable to process. </p>");
      }

      out.println("  </body>");
      out.println("</html>");
      out.close();
      session.close();
      return;
    }
    doGet(request, response);

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
    profile.getOrganization().getOrgLabel(); // did this to force lazy loading now
    return profile;
  }

}
