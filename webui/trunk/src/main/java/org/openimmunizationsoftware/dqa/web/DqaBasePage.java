/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web;


import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Session;

public class DqaBasePage extends WebPage
{
  public DqaBasePage() {
    this(new PageParameters(), NavigationPanel.HOME_LOGGED_IN);
  }

  public DqaBasePage(final PageParameters parameters, NavigationPanel.MenuLink menuLinkSelected) {
    add(new NavigationPanel("navigationPanel", menuLinkSelected));
    add(new VersionPanel("versionPanel"));
  }
  
  protected DqaSession getWebSession()
  {
    return (DqaSession) getSession();
  }
  
  protected Session getDataSession()
  {
    return getWebSession().getDataSession();
  }
  
  protected static Label makeSelected(Label label, boolean selected)
  {
    if (selected)
    {
      label.add(new SimpleAttributeModifier("class", "selected"));
    }
    return label;
  }
}
