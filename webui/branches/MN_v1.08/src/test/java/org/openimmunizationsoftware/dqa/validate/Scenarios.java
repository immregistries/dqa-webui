/*
 * Copyright 2013 by Dandelion Software & Research, Inc (DSR)
 * 
 * This application was written for immunization information system (IIS) community and has
 * been released by DSR under an Apache 2 License with the hope that this software will be used
 * to improve Public Health.  
 */
package org.openimmunizationsoftware.dqa.validate;

import junit.framework.TestCase;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openimmunizationsoftware.dqa.db.model.IssueFound;
import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.PotentialIssue;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.manager.OrganizationManager;
import org.openimmunizationsoftware.dqa.manager.PotentialIssues;
import org.openimmunizationsoftware.dqa.parse.VaccinationParserHL7;
import org.openimmunizationsoftware.dqa.validate.Validator;

public class Scenarios extends TestCase
{
  private static final String NO_PROBLEMS = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_3_B = "MSH|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_3_C = "MSH|^~\\||ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_3_D = "MSH|^~\\&-ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_3_E = "MSH|^~&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_4_A = "MSH|^~\\&|ABC||||20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_4_B = "MSH|^~\\&|ABC||||20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  private static final String SCENARIO_4_C = "MSH|^~\\&||ABC|||20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";
  private static final String SCENARIO_4_D = "MSH|^~\\&|||ABC||20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";
  private static final String SCENARIO_4_E = "MSH|^~\\&||||ABC|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  /**
   * Tests to make sure parser can pickup when there are issues with the MSH
   * encoding errors
   * 
   * @throws Exception
   */
  public void testScenario3() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);

    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    assertFalse(messageReceived.hasErrors());

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_3_B);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    assertTrue(messageReceived.hasErrors());
    assertTrue(hasError(messageReceived, pi.Hl7MshEncodingCharacterIsInvalid));

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_3_C);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    assertTrue(messageReceived.hasErrors());
    assertTrue(hasError(messageReceived, pi.Hl7MshEncodingCharacterIsInvalid));

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_3_D);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    assertTrue(messageReceived.hasErrors());
    assertTrue(hasError(messageReceived, pi.Hl7MshEncodingCharacterIsInvalid));

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_3_E);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    assertTrue(messageReceived.hasErrors());
    assertTrue(hasError(messageReceived, pi.Hl7MshEncodingCharacterIsInvalid));
    tx.rollback();
    session.close();
  }

  public void testScenario4() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    printErrors(messageReceived);
    assertFalse(messageReceived.hasErrors());
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_4_B);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.Hl7MshSendingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingFacilityIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_4_C);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingApplicationIsMissing));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingFacilityIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_4_D);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsMissing));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshReceivingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingFacilityIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_4_E);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingApplicationIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshReceivingApplicationIsMissing));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshReceivingFacilityIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_5_A = "MSH|^~\\&|ABC|TCHHO|TxImmTrac|TxDSHS|20111311033355.888||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario5() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_5_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsInvalid));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshMessageDateIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.Hl7MshSendingFacilityIsInvalid));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshMessageDateIsInvalid));
    tx.rollback();
    session.close();
  }

  private static final String SCENARIO_6_A = "MSH|^~\\&|ABC|6666666666|TxImmTrac|TxDSHS|20110801092154||VXU^V04||||\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario6() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.Hl7MshProcessingIdIsMissing));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshMessageControlIdIsMissing));
    assertFalse(hasIssue(messageReceived, pi.Hl7MshVersionIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_6_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.Hl7MshProcessingIdIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshMessageControlIdIsMissing));
    assertTrue(hasIssue(messageReceived, pi.Hl7MshVersionIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_9_A = "MSH|^~\\&|ABC|9999999999|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~12345678^^^^MA||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario9() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.PatientMedicaidNumberIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_9_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.PatientMedicaidNumberIsInvalid));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_10_A = "MSH|^~\\&|ABC|1010101010|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~12345678^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario10() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.PatientSsnIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_10_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.PatientSsnIsInvalid));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_13_A = "MSH|^~\\&|ABC|1212121212|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname|||U||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario13() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.PatientBirthDateIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_13_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.PatientBirthDateIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_14_A = "MSH|^~\\&|ABC|1212121212|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20111301|U||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario14() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.PatientBirthDateIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_14_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.PatientBirthDateIsInvalid));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_21_A = "MSH|^~\\&|ABC|2121212121|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1||^Mother^HL70063|\r"
      + "PV1||R||||||||||||||||||V00|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario21() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.PatientGuardianNameFirstIsMissing));
    assertFalse(hasIssue(messageReceived, pi.PatientGuardianNameLastIsMissing));
    assertFalse(hasIssue(messageReceived, pi.PatientGuardianRelationshipIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_21_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.PatientGuardianNameFirstIsMissing));
    assertTrue(hasIssue(messageReceived, pi.PatientGuardianNameLastIsMissing));
    assertTrue(hasIssue(messageReceived, pi.PatientGuardianRelationshipIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_25_A = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20101133|20101133|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario25() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.VaccinationAdminDateIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_25_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.VaccinationAdminDateIsInvalid));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_27_A = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214||999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario27() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.VaccinationAdminCodeIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_27_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.VaccinationAdminCodeIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_28_A = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|405^Bad Code^CVX|999|||00^New immunization record^NIP001||^^^1124800189^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario28() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.VaccinationAdminCodeIsUnrecognized));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_28_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.VaccinationAdminCodeIsUnrecognized));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_30_A = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001|||\r";

  public void testScenario30() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.VaccinationFacilityIdIsMissing));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_30_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.VaccinationFacilityIdIsMissing));
    tx.rollback();

    session.close();
  }

  private static final String SCENARIO_31_A = "MSH|^~\\&|ABC|1124800189|TxImmTrac|TxDSHS|20110801092154||VXU^V04|1312208514674.1002|P|2.4|\r"
      + "PID|||100755502^^^^PI~122112219^^^^MA~123123124^^^^SS||Lastname^Firstname||20011118|F||2131-1^Other Race^HL70005|6118 SAN FELIPE^^DALLAS^TX^77057^US^P^^TX201||^PRN^^^^713^9876543|||||||||||N|\r"
      + "NK1|1|Mom LastNme^Mom Firstname|MTH^Mother^HL70063|\r"
      + "PV1||R|\r"
      + "RXA|0|999|20021214|20021214|21^varicella^CVX^90716^Varicella^C4|999|||00^New immunization record^NIP001||^^^BADID^^^^^3131 Briarpark Dr., Ste 108^^Dallas^TX^77042^US|\r";

  public void testScenario31() throws Exception
  {
    SessionFactory factory = OrganizationManager.getSessionFactory();
    Session session = factory.openSession();
    Transaction tx = session.beginTransaction();

    SubmitterProfile profile = (SubmitterProfile) session.get(SubmitterProfile.class, 253);
    profile.initPotentialIssueStatus(session);
    profile.setDataFormat(SubmitterProfile.DATA_FORMAT_HL7V2);
    VaccinationParserHL7 parser = new VaccinationParserHL7(profile);
    MessageReceived messageReceived;
    PotentialIssues pi = PotentialIssues.getPotentialIssues();
    Validator validator = new Validator(profile, session);

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(NO_PROBLEMS);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertFalse(hasIssue(messageReceived, pi.VaccinationFacilityIdIsInvalid));
    tx.rollback();

    tx = session.beginTransaction();
    messageReceived = new MessageReceived();
    messageReceived.setRequestText(SCENARIO_31_A);
    messageReceived.setProfile(profile);
    parser.createVaccinationUpdateMessage(messageReceived);
    validator.validateVaccinationUpdateMessage(messageReceived);
    assertTrue(hasIssue(messageReceived, pi.VaccinationFacilityIdIsInvalid));
    tx.rollback();

    session.close();
  }

  private void printErrors(MessageReceived messageReceived)
  {
    for (IssueFound issueFound : messageReceived.getIssuesFound())
    {
      if (issueFound.isError())
      {
        System.out.println("--> " + issueFound.getDisplayText());
      }
    }
  }

  private boolean hasIssue(MessageReceived messageReceived, PotentialIssue potentialIssue)
  {
    boolean foundIssue = false;
    for (IssueFound issueFound : messageReceived.getIssuesFound())
    {
      if (issueFound.getIssue() == potentialIssue)
      {
        foundIssue = true;
      }
    }
    return foundIssue;
  }

  private boolean hasError(MessageReceived messageReceived, PotentialIssue potentialIssue)
  {
    boolean foundError = false;
    for (IssueFound issueFound : messageReceived.getIssuesFound())
    {
      if (issueFound.isError())
      {
        if (issueFound.getIssue() == potentialIssue)
        {
          foundError = true;
        }
      }
    }
    return foundError;
  }
}
