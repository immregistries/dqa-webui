package org.openimmunizationsoftware.dqa.web;

import java.util.List;
import java.util.Locale;

import org.apache.wicket.Session;
import org.apache.wicket.protocol.http.WebSession;
import org.apache.wicket.request.Request;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.db.model.UserAccount;

public class DqaSession extends WebSession
{

  private transient org.hibernate.Session dataSession = null;

  public static DqaSession get()
  {
    return (DqaSession) Session.get();
  }

  private String username = null;
  private boolean admin = false;
  private int profileId = 0;
  private int receivedId = 0;
  private int batchId = 0;

  public int getBatchId()
  {
    return batchId;
  }

  public void setBatchId(int batchId)
  {
    this.batchId = batchId;
  }

  public int getReceivedId()
  {
    return receivedId;
  }

  public void setReceivedId(int receivedId)
  {
    this.receivedId = receivedId;
  }

  private NavigationPanel.MenuLink menuLinkSelected = null;

  public UserAccount getUserAccount()
  {
    Query query = getDataSession().createQuery("from UserAccount where username = ?");
    query.setParameter(0, username);
    List<UserAccount> userAccountList = query.list();
    if (userAccountList.size() > 0)
    {
      return userAccountList.get(0);
    }
    return null;
  }

  public int getProfileId()
  {
    return profileId;
  }

  public void setProfileId(int profileId)
  {
    this.profileId = profileId;
  }
  
  public SubmitterProfile getSubmitterProfile()
  {
    if (profileId > 0)
    {
      return (SubmitterProfile) getDataSession().get(SubmitterProfile.class, profileId);
    }
    return null;
  }

  public boolean isAdmin()
  {
    return admin;
  }

  public void setAdmin(boolean admin)
  {
    this.admin = admin;
  }

  public NavigationPanel.MenuLink getMenuLinkSelected()
  {
    return menuLinkSelected;
  }

  public void setMenuLinkSelected(NavigationPanel.MenuLink menuLinkSelected)
  {
    this.menuLinkSelected = menuLinkSelected;
  }

  public boolean isLoggedIn()
  {
    return username != null;
  }

  public String getUsername()
  {
    return username;
  }

  public void setUsername(String username)
  {
    this.username = username;
  }

  public DqaSession(Request request) {
    super(request);
    setLocale(Locale.ENGLISH);
  }

  @Override
  public void invalidate()
  {
    if (dataSession != null && dataSession.isOpen())
    {
      dataSession.close();
    }
    dataSession = null;
    super.invalidate();
  }

  public org.hibernate.Session getDataSession()
  {
    if (dataSession == null)
    {
      SessionFactory factory = CentralControl.getSessionFactory();
      dataSession = factory.openSession();
    }
    return dataSession;
  }

}
