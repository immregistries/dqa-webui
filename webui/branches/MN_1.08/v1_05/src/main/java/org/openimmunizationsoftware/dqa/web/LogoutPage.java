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
