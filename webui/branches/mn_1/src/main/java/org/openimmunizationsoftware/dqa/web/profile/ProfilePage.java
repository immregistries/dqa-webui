package org.openimmunizationsoftware.dqa.web.profile;

import org.apache.wicket.RestartResponseException;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.DqaSession;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class ProfilePage extends DqaBasePage implements SecurePage
{

  public ProfilePage() {
    this(new PageParameters());
  }

  public ProfilePage(final PageParameters parameters) {
    super(parameters, NavigationPanel.PROFILE);

    DqaSession webSession = (DqaSession) getSession();

    SubmitterProfile profile = webSession.getSubmitterProfile();
    if (profile == null)
    {
      throw new RestartResponseException(new ProfileSelectPage());
    }
    
    add(new Label("profileId", ""+ profile.getProfileId()));
    add(new Label("profileCode", profile.getProfileCode()));
    add(new Label("profileLabel", profile.getProfileLabel()));
    add(new Label("profileStatus", profile.getProfileStatus()));
    add(new Label("organizationLabel", profile.getOrganization().getOrgLabel()));
    add(new Label("dataFormat", profile.getDataFormat()));
    add(new Label("transferPriority", profile.getTransferPriority()));
    add(new Label("reportTemplateLabel", profile.getReportTemplate().getTemplateLabel()));
    
  }
}