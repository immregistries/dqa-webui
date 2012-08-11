package org.openimmunizationsoftware.dqa.web.batch;

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class BatchPage extends DqaBasePage implements SecurePage
{
  public BatchPage() {
    this(new PageParameters());
  }

  public BatchPage(PageParameters pageParameters) {
    super(pageParameters, NavigationPanel.BATCH);
  }
}
