package org.openimmunizationsoftware.dqa.web.batch;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class ErrorsWarningsPage extends DqaBasePage  implements SecurePage
{

  public ErrorsWarningsPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.BATCH);
  }
}
