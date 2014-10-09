package org.openimmunizationsoftware.dqa.web;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextArea;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.model.Model;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;

/**
 * Homepage
 */
public class Submit extends WebPage
{

  private static final long serialVersionUID = 1L;

  // TODO Add any page properties or variables here
  private TextField<String> orgLabel;
  private TextArea<String> message;

  private String results = "";
  
  /**
   * Constructor that is invoked when page is invoked without a session.
   * 
   * @param parameters
   *          Page parameters
   */
  public Submit(final PageParameters parameters) {

    Form form = new Form("form");
    orgLabel = new TextField<String>("orgLabel", new Model<String>(""));
    message = new TextArea<String>("message", new Model<String>(""));
    form.add(orgLabel);
    form.add(message);
    form.add(new Button("button")
    {
      @Override
      public void onSubmit()
      {
        String orgLabelString = orgLabel.getModelObject();
        String messageString = message.getModelObject();
        
        SessionFactory factory = OrganizationManager.getSessionFactory();
        Session session = factory.openSession();
        Transaction tx = session.beginTransaction();
        Query query = session.createQuery("from Organization where orgLabel = ?");
        query.setParameter(0, orgLabelString);
        List<Organization> organizations = query.list();
        Organization organization;
        if (organizations.size() == 0)
        {
          organization = new Organization();
          organization.setParentOrganization((Organization)session.get(Organization.class, 1));
          organization.setOrgLabel(orgLabelString);
          session.save(organization);
        }
        else
        {
          organization = organizations.get(0);
        }
        SubmitterProfile submitterProfile = organization.getPrimaryProfile();
        if (submitterProfile == null)
        {
          submitterProfile = new SubmitterProfile();
          submitterProfile.setProfileLabel("HL7");
          submitterProfile.setProfileStatus(SubmitterProfile.PROFILE_STATUS_TEST);
          submitterProfile.setOrganization(organization);
          submitterProfile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
          submitterProfile.setTransferPriority(SubmitterProfile.TRANSFER_PRIORITY_NORMAL);
          session.save(submitterProfile);
          organization.setPrimaryProfile(submitterProfile);
        }
        tx.commit();
        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter(stringWriter);
        try 
        {
          // TODO IncomingServlet.processStream(false, out, session, submitterProfile, messageString);
        }
        catch (Exception e)
        {
          e.printStackTrace(out);
        }
        out.close();
        results = stringWriter.toString();
      }
    });
    add(new Label("results"));
    add(form);
  }
}
