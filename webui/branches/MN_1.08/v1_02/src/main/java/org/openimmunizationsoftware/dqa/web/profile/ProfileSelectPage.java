package org.openimmunizationsoftware.dqa.web.profile;

import java.util.List;

import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Query;
import org.hibernate.Session;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.DqaSession;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;
import org.openimmunizationsoftware.dqa.web.batch.DataReceivedPage;

public class ProfileSelectPage extends DqaBasePage implements SecurePage
{

  public ProfileSelectPage() {
    this(new PageParameters());
  }

  public ProfileSelectPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.PROFILE);

    DqaSession webSession = (DqaSession) getSession();

    List<SubmitterProfile> submitterProfileList;

    Session dataSession = webSession.getDataSession();

    if (webSession.isAdmin())
    {
      Query query = dataSession.createQuery("from SubmitterProfile where reportTemplate.templateId != 0 order by profileLabel");
      submitterProfileList = query.list();
    } else
    {
      Query query = dataSession.createQuery("from SubmitterProfile where organization = ?");
      query.setParameter(0, webSession.getUserAccount().getOrganization());
      submitterProfileList = query.list();
    }

    SubmitterProfile submitterProfileSelected = webSession.getSubmitterProfile();

    ListView<SubmitterProfile> submitterProfileItems = new ListView<SubmitterProfile>("submitterProfileItems", submitterProfileList) {
      @Override
      protected void populateItem(ListItem<SubmitterProfile> item)
      {
        final SubmitterProfile submitterProfile = item.getModelObject();
        final SubmitterProfile submitterProfileSelected = ((DqaSession) getSession()).getSubmitterProfile();
        if (submitterProfileSelected != null && submitterProfile.equals(submitterProfileSelected))
        {
          item.add(new Label("profileLabel", submitterProfile.getProfileLabel()).add(new SimpleAttributeModifier("class", "selected")));
          item.add(new Label("orgLabel", submitterProfile.getOrganization().getOrgLabel()).add(new SimpleAttributeModifier("class", "selected")));
          item.add(new Label("profileStatus", submitterProfile.getProfileStatus()).add(new SimpleAttributeModifier("class", "selected")));
          item.add(new Label("templateLabel", submitterProfile.getReportTemplate().getTemplateLabel()).add(new SimpleAttributeModifier("class",
              "selected")));
        } else
        {
          item.add(new Label("profileLabel", submitterProfile.getProfileLabel()));
          item.add(new Label("orgLabel", submitterProfile.getOrganization().getOrgLabel()));
          item.add(new Label("profileStatus", submitterProfile.getProfileStatus()));
          item.add(new Label("templateLabel", submitterProfile.getReportTemplate().getTemplateLabel()));
        }
        Link link = new Link("submitterProfileSelectLink") {
          @Override
          public void onClick()
          {
            DqaSession webSession = (DqaSession) getSession();
            webSession.setProfileId(submitterProfile.getProfileId());
            setResponsePage(new DataReceivedPage(parameters));
          }
        };
        item.add(link);
      }
    };
    add(submitterProfileItems);

  }
}