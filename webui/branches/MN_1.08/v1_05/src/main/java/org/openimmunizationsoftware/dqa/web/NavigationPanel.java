package org.openimmunizationsoftware.dqa.web;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.admin.AdminPage;
import org.openimmunizationsoftware.dqa.web.admin.AdminVaccineCodesPage;
import org.openimmunizationsoftware.dqa.web.batch.BatchPage;
import org.openimmunizationsoftware.dqa.web.batch.DataReceivedPage;
import org.openimmunizationsoftware.dqa.web.batch.ErrorsWarningsPage;
import org.openimmunizationsoftware.dqa.web.batch.WeeklyReportPage;
import org.openimmunizationsoftware.dqa.web.profile.ProfileDocumentationPage;
import org.openimmunizationsoftware.dqa.web.profile.ProfilePage;
import org.openimmunizationsoftware.dqa.web.profile.ProfileSelectPage;
import org.openimmunizationsoftware.dqa.web.profile.ProfileSettingsPage;

public class NavigationPanel extends Panel
{
  private final class MenuListView extends ListView<MenuLink>
  {
    private MenuListView(String id, List<? extends MenuLink> list) {
      super(id, list);
    }

    @Override
    protected void populateItem(ListItem<MenuLink> item)
    {
      MenuLink menuLink = item.getModelObject();
      DqaSession webSession = (DqaSession) getSession();
      BookmarkablePageLink link = new BookmarkablePageLink("link", menuLink.getPageClass(), menuLink.getPageParameters());
      item.add(link);
      link.add(new Label("label", menuLink.getLinkLabel()));
      if (menuLink.isSelected())
      {
        link.add(new SimpleAttributeModifier("id", "current"));
      }

      ListView<MenuLink> childMenuLinkItems = new ListView<MenuLink>("childMenuLinkItems", menuLink.childList) {
        @Override
        protected void populateItem(ListItem<MenuLink> item)
        {
          MenuLink menuLink = item.getModelObject();
          BookmarkablePageLink link = new BookmarkablePageLink("childLink", menuLink.getPageClass(), menuLink.getPageParameters());
          item.add(link);
          link.add(new Label("childLabel", menuLink.getLinkLabel()));
        }
      };
      item.add(childMenuLinkItems);

    }
  }

  public static class MenuLink implements Serializable
  {
    private String linkLabel = "";
    private Class pageClass = null;
    private boolean selected = false;
    private List<MenuLink> childList = new ArrayList<NavigationPanel.MenuLink>();
    private PageParameters pageParameters = new PageParameters();

    public MenuLink(MenuLink menuLinkCopy) {
      this.linkLabel = menuLinkCopy.linkLabel;
      this.pageClass = menuLinkCopy.pageClass;
      childList = new ArrayList<NavigationPanel.MenuLink>(menuLinkCopy.childList);
    }

    public void add(MenuLink menuLink)
    {
      childList.add(menuLink);
    }

    public PageParameters getPageParameters()
    {
      return pageParameters;
    }

    public void setPageParameters(PageParameters pageParameters)
    {
      this.pageParameters = pageParameters;
    }

    public List<MenuLink> getChildList()
    {
      return childList;
    }

    public boolean isSelected()
    {
      return selected;
    }

    public void setSelected(boolean selected)
    {
      this.selected = selected;
    }

    public String getLinkLabel()
    {
      return linkLabel;
    }

    public void setLinkLabel(String linkLabel)
    {
      this.linkLabel = linkLabel;
    }

    public Class getPageClass()
    {
      return pageClass;
    }

    public void setPageClass(Class pageClass)
    {
      this.pageClass = pageClass;
    }

    private MenuLink(String linkLabel, Class pageClass) {
      this.linkLabel = linkLabel;
      this.pageClass = pageClass;
    }

  }

  public static MenuLink HOME_LOGGED_OUT = new MenuLink("DQA", HomePage.class);
  public static MenuLink LOGIN = new MenuLink("Log In", LoginPage.class);

  public static MenuLink HOME_LOGGED_IN = new MenuLink("DQA", HomePage.class);
  public static MenuLink LOGOUT = new MenuLink("Log Out", LogoutPage.class);
  public static MenuLink PERSONAL_SETTINGS = new MenuLink("Settings", PersonalSettingsPage.class);

  public static MenuLink PROFILE = new MenuLink("Profile", ProfilePage.class);
  public static MenuLink PROFILE_DOCUMENTATION = new MenuLink("Documentation", ProfileDocumentationPage.class);
  public static MenuLink PROFILE_SETTINGS = new MenuLink("Settings", ProfileSettingsPage.class);
  public static MenuLink PROFILE_SELECT = new MenuLink("Select", ProfileSelectPage.class);
  public static MenuLink UPLOAD_NOW = new MenuLink("Upload Now", HomePage.class);

  public static MenuLink BATCH = new MenuLink("Batch", BatchPage.class);
  public static MenuLink DATA_RECEIVED = new MenuLink("Data Received", DataReceivedPage.class);
  public static MenuLink ERRORS_WARNINGS = new MenuLink("Errors & Warnings", ErrorsWarningsPage.class);
  public static MenuLink WEEKLY_REPORT = new MenuLink("Weekly Report", WeeklyReportPage.class);

  public static MenuLink ADMIN = new MenuLink("Admin", AdminPage.class);
  public static MenuLink ADMIN_VACCINE_CODES = new MenuLink("Vaccine Codes", AdminVaccineCodesPage.class);

  private List<MenuLink> createMenu(DqaSession webSession)
  {
    List<MenuLink> menuLinkList = new ArrayList<NavigationPanel.MenuLink>();
    if (webSession.isLoggedIn())
    {
      MenuLink home = new MenuLink(HOME_LOGGED_IN);
      menuLinkList.add(home);
      home.getChildList().add(PERSONAL_SETTINGS);
      home.getChildList().add(LOGOUT);

      {
        // Profile Menu
        MenuLink profile = new MenuLink(PROFILE);
        menuLinkList.add(profile);

        profile.add(PROFILE_SELECT);
        if (webSession.getSubmitterProfile() != null)
        {
          profile.add(PROFILE_DOCUMENTATION);
          profile.add(UPLOAD_NOW);
          if (webSession.isAdmin())
          {
            profile.add(PROFILE_SETTINGS);
          }
        }
      }

      if (webSession.getSubmitterProfile() != null)
      {
        // Profile Menu
        MenuLink batch = new MenuLink(BATCH);
        menuLinkList.add(batch);
        batch.add(DATA_RECEIVED);
        batch.add(ERRORS_WARNINGS);
        batch.add(WEEKLY_REPORT);
      }
      if (webSession.isAdmin())
      {
        MenuLink admin = new MenuLink(ADMIN);
        menuLinkList.add(admin);
        
        admin.add(ADMIN_VACCINE_CODES);
      }
    } else
    {
      menuLinkList.add(HOME_LOGGED_OUT);
      menuLinkList.add(LOGIN);
    }
    return menuLinkList;
  }

  private List<MenuLink> createSideMenu(DqaSession webSession)
  {
    List<MenuLink> menuLinkList = new ArrayList<NavigationPanel.MenuLink>();
    // SubmitterProfile submitterProfile = webSession.getSubmitterProfile();
    // if (submitterProfile != null)
    // {
    // MenuLink profile = new MenuLink(PROFILE);
    // menuLinkList.add(profile);
    // profile.getChildList().add(DATA_RECEIVED);
    // profile.getChildList().add(ERRORS_WARNINGS);
    // profile.getChildList().add(WEEKLY_REPORT);
    // if (webSession.isAdmin())
    // {
    // profile.getChildList().add(PROFILE_SETTINGS);
    // }
    // profile.getChildList().add(UPLOAD_NOW);
    // }
    return menuLinkList;
  }

  public NavigationPanel(String id, MenuLink menuLinkSelected) {
    super(id);
    DqaSession webSession = (DqaSession) getSession();
    List<MenuLink> menuLinkList = createMenu(webSession);
    if (menuLinkSelected == null)
    {
      menuLinkSelected = HOME_LOGGED_OUT;
    }
    for (MenuLink menuLink : menuLinkList)
    {
      menuLink.setSelected(menuLinkSelected.getLinkLabel().equals(menuLink.getLinkLabel()));
    }
    webSession.setMenuLinkSelected(menuLinkSelected);

    ListView<MenuLink> menuLinkItems = new MenuListView("menuLinkItems", menuLinkList);
    add(menuLinkItems);

    WebMarkupContainer sideMenuSection = new WebMarkupContainer("sidemenu");
    add(sideMenuSection);

    List<MenuLink> sideMenuLinkList = createSideMenu(webSession);
    if (sideMenuLinkList.size() == 0)
    {
      sideMenuSection.setVisible(false);
    } else
    {
      ListView<MenuLink> sideMenuLinkItems = new MenuListView("menuLinkItems", sideMenuLinkList);
      sideMenuSection.add(sideMenuLinkItems);
    }

  }

}
