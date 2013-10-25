package org.openimmunizationsoftware.dqa.web.admin;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class AdminPage extends DqaBasePage implements SecurePage
{

  public AdminPage() {
    this(new PageParameters());
  }

  public AdminPage(PageParameters pageParameters) {
    super(pageParameters, NavigationPanel.ADMIN);
  }
}
