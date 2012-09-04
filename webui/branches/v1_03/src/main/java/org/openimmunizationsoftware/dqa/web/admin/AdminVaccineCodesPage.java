package org.openimmunizationsoftware.dqa.web.admin;

import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class AdminVaccineCodesPage extends DqaBasePage implements SecurePage
{
  public AdminVaccineCodesPage() {
    this(new PageParameters());
  }

  public AdminVaccineCodesPage(PageParameters pageParameters) {
    super(pageParameters, NavigationPanel.ADMIN);
    
    add(new BookmarkablePageLink("adminCvxPageLink", AdminCvxPage.class));
    add(new BookmarkablePageLink("adminCptPageLink", AdminCptPage.class));

  }
}
