package org.openimmunizationsoftware.dqa.web;


import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.Organization;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;

/**
 * Homepage
 */
public class HomePage extends DqaBasePage 
{

  private static final long serialVersionUID = 1L;
 

  // TODO Add any page properties or variables here

  /**
   * Constructor that is invoked when page is invoked without a session.
   * 
   * @param parameters
   *          Page parameters
   */
  public HomePage(final PageParameters parameters) {
    super(parameters, NavigationPanel.HOME_LOGGED_IN);

    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    Organization organization = (Organization) session.get(Organization.class, 1);
    SubmitterProfile submitterProfile = organization.getPrimaryProfile();
    // Add the simplest type of label
    add(new Label("messageField", "Hello, primary organization = " + organization.getOrgLabel() + ""));
    tx.commit();
    session.close();
  }
  
}
