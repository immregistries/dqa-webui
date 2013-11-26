/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.web;

import org.apache.wicket.request.mapper.parameter.PageParameters;

public class LogoutPage extends DqaBasePage implements SecurePage
{

  public LogoutPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.LOGOUT);
    DqaSession webSession = (DqaSession) getSession();
    webSession.invalidate();
    webSession.setUsername(null);
    webSession.setAdmin(false);
    setResponsePage(getApplication().getHomePage());
  }
}
