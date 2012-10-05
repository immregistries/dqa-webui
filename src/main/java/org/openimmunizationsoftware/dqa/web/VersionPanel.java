package org.openimmunizationsoftware.dqa.web;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.Panel;
import org.hibernate.Session;
import org.openimmunizationsoftware.dqa.manager.DatabaseLogManager;

public class VersionPanel extends Panel
{

  public VersionPanel(String id) {
    super(id);
    DqaSession webSession = (DqaSession) getSession();
    Session dataSession = webSession.getDataSession();

    Label versionLabel = new Label("version", DatabaseLogManager.getVersionDescription(dataSession));
    add(versionLabel);

  }

}
