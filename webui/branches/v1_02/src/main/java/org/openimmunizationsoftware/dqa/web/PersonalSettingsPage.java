package org.openimmunizationsoftware.dqa.web;

import org.apache.wicket.request.mapper.parameter.PageParameters;

public class PersonalSettingsPage extends DqaBasePage  implements SecurePage
{

  public PersonalSettingsPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.HOME_LOGGED_IN);
  }
}
