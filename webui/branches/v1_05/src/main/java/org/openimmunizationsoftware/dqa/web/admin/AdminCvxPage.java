package org.openimmunizationsoftware.dqa.web.admin;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class AdminCvxPage extends DqaBasePage implements SecurePage
{

  public AdminCvxPage()
  {
    this(new PageParameters());
  }
  
  public AdminCvxPage(PageParameters pageParameters)
  {
    super(pageParameters, NavigationPanel.ADMIN);
  }
}
