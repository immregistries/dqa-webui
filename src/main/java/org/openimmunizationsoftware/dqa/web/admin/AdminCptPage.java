package org.openimmunizationsoftware.dqa.web.admin;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class AdminCptPage extends DqaBasePage implements SecurePage
{
  public AdminCptPage()
  {
    this(new PageParameters());
  }
  
  public AdminCptPage(PageParameters pageParameters)
  {
    super(pageParameters, NavigationPanel.ADMIN);
  }
}
