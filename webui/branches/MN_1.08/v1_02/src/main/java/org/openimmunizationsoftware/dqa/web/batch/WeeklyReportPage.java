package org.openimmunizationsoftware.dqa.web.batch;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class WeeklyReportPage extends DqaBasePage  implements SecurePage
{

  public WeeklyReportPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.BATCH);
  }
}
