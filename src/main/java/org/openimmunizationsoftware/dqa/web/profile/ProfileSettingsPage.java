package org.openimmunizationsoftware.dqa.web.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.Model;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.ResetProfileManager;
import org.openimmunizationsoftware.dqa.web.DqaBasePage;
import org.openimmunizationsoftware.dqa.web.DqaSession;
import org.openimmunizationsoftware.dqa.web.NavigationPanel;
import org.openimmunizationsoftware.dqa.web.SecurePage;

public class ProfileSettingsPage extends DqaBasePage implements SecurePage
{

  private Model<String> profileLabelModel = null;
  private Model<String> profileStatusModel = null;
  private Model<String> dataFormatModel = null;
  private Model<String> transferPriorityModel = null;
  
  public ProfileSettingsPage()
  {
    this(new PageParameters());
  }

  public ProfileSettingsPage(final PageParameters parameters) {
    super(parameters, NavigationPanel.PROFILE);

    DqaSession webSession = (DqaSession) getSession();
    final SubmitterProfile submitterProfile = webSession.getSubmitterProfile();

    FeedbackPanel feedback = new FeedbackPanel("msgs");
    add(feedback);

    Form<Void> form = new Form<Void>("form") {
      private static final long serialVersionUID = 1L;

      @Override
      protected void onSubmit()
      {
        DqaSession webSession = getWebSession();
        Session dataSession = getDataSession();
        Transaction transaction = dataSession.beginTransaction();
        SubmitterProfile submitterProfile = webSession.getSubmitterProfile();
        submitterProfile.setProfileLabel(profileLabelModel.getObject());
        submitterProfile.setProfileStatus(profileStatusModel.getObject());
        dataSession.update(submitterProfile);
        transaction.commit();
      }
    };
    add(form);
    form.add(new Label("profileId", "" + submitterProfile.getProfileId()));
    form.add(new Label("profileCode", "" + submitterProfile.getProfileCode()));

    profileLabelModel = new Model<String>(submitterProfile.getProfileLabel());
    TextField<String> profileLabel = new TextField<String>("profileLabel", profileLabelModel);
    profileLabel.setRequired(true);
    form.add(profileLabel);

    profileStatusModel = new Model<String>(submitterProfile.getProfileStatus());
    List<String> profileStatusList = new ArrayList<String>();
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_SETUP);
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_TEST);
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_PROD);
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_HOLD);
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_CLOSED);
    profileStatusList.add(SubmitterProfile.PROFILE_STATUS_TEMPLATE);
    DropDownChoice<String> profileStatusChoice = new DropDownChoice<String>("profileStatus", profileStatusModel, profileStatusList);
    form.add(profileStatusChoice);

    form.add(new Label("organizationLabel", "" + submitterProfile.getOrganization().getOrgLabel()));

    dataFormatModel = new Model<String>(submitterProfile.getDataFormat());
    List<String> dataFormatList = new ArrayList<String>();
    dataFormatList.add(SubmitterProfile.DATA_FORMAT_HL7V2);
    DropDownChoice<String> dataFormatChoice = new DropDownChoice<String>("dataFormat", dataFormatModel, dataFormatList);
    form.add(dataFormatChoice);

    transferPriorityModel = new Model<String>(submitterProfile.getTransferPriority());
    List<String> transferPriorityList = new ArrayList<String>();
    transferPriorityList.add(SubmitterProfile.TRANSFER_PRIORITY_HIGHEST);
    transferPriorityList.add(SubmitterProfile.TRANSFER_PRIORITY_HIGH);
    transferPriorityList.add(SubmitterProfile.TRANSFER_PRIORITY_NORMAL);
    transferPriorityList.add(SubmitterProfile.TRANSFER_PRIORITY_LOW);
    transferPriorityList.add(SubmitterProfile.TRANSFER_PRIORITY_LOWEST);
    DropDownChoice<String> transferPriority = new DropDownChoice<String>("transferPriority", transferPriorityModel, transferPriorityList);
    form.add(transferPriority);

    Link<Void> resetProfileLink = new Link<Void>("resetProfileLink") {
      private static final long serialVersionUID = 1L;
      public void onClick()
      {
        ResetProfileManager rpm = new ResetProfileManager();
        rpm.resetProfile(submitterProfile, getDataSession());
        setResponsePage(new ProfileSettingsPage());
      }
    };
    add(resetProfileLink);
  }
}
