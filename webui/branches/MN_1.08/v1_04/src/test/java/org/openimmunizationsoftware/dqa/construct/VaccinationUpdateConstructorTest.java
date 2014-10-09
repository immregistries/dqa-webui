package org.openimmunizationsoftware.dqa.construct;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import junit.framework.TestCase;

import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.db.model.received.NextOfKin;
import org.openimmunizationsoftware.dqa.db.model.received.Patient;
import org.openimmunizationsoftware.dqa.db.model.received.Vaccination;

public class VaccinationUpdateConstructorTest extends TestCase
{
  public void testConstructMessage()
  {
    SubmitterProfile profile = new SubmitterProfile();
    VaccinationUpdateConstructer vuc = new VaccinationUpdateConstructer(profile);
    MessageReceived messageReceived = new MessageReceived();
    populateMessage(messageReceived);
    String message = vuc.constructMessage(messageReceived);
    assertNotNull(message);
    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmssZ");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyMMddHHmmss");
    assertEquals(
        message,
        "MSH|^~\\&|DQA||TxImmTrac|TxDSHS|"
            + sdf1.format(messageReceived.getReceivedDate())
            + "||VXU^V04|1-"
            + sdf2.format(vuc.getGenerationDate())
            + "|P|2.5|\r"
            + "PID|||MRN~SSN~MEDICAID||Last^First^M^Jr^Dr^^L|Maiden|20010210|M||Race|123 E Main^Apt A^Anytown^TX^85844^USA^M^^County||(108)555-1533^^^^^108^5551533|||||||||Ethnicity||Y|2|\r"
            + "NK1|1|Mother Last^Mother First|MTH|\r" + "PV1||R||||||||||||||||||V01|\r" + "ORC|RE|Submitter Id|\r"
            + "RXA|0|999|20110718||90703^^C4|0.5|ML||01|Given By|^^^Facility ID||||Lot Number||Manufacturer|\r");
  }

  private void populateMessage(MessageReceived messageReceived)
  {
    Patient patient = messageReceived.getPatient();
    patient.setAddressCity("Anytown");
    patient.setAddressCountryCode("USA");
    patient.setAddressCountyParishCode("County");
    patient.setAddressStateCode("TX");
    patient.setAddressStreet("123 E Main");
    patient.setAddressStreet2("Apt A");
    patient.setAddressTypeCode("M");
    patient.setAddressZip("85844");
    patient.setBirthDate(makeDate(2001, 2, 10));
    patient.setBirthMultiple("Y");
    patient.setBirthOrderCode("2");
    patient.setEthnicityCode("Ethnicity");
    patient.setFinancialEligibilityCode("V01");
    patient.setIdMedicaidNumber("MEDICAID");
    patient.setIdSsnNumber("SSN");
    patient.setIdSubmitterNumber("MRN");
    patient.setMotherMaidenName("Maiden");
    patient.setNameFirst("First");
    patient.setNameLast("Last");
    patient.setNameMiddle("M");
    patient.setNamePrefix("Dr");
    patient.setNameSuffix("Jr");
    patient.setNameTypeCode("L");
    patient.setPhoneNumber("(108)555-1533");
    patient.setRaceCode("Race");
    patient.setSexCode("M");
    NextOfKin nextOfKin = new NextOfKin();
    messageReceived.getNextOfKins().add(nextOfKin);
    nextOfKin.setNameFirst("Mother First");
    nextOfKin.setNameLast("Mother Last");
    nextOfKin.setRelationshipCode("MTH");
    Vaccination vaccination = new Vaccination();
    messageReceived.getVaccinations().add(vaccination);
    vaccination.setAdminCptCode("90703");
    vaccination.getAdminCpt().setText("Text");
    vaccination.setAdminDate(makeDate(2011, 7, 18));
    vaccination.setAmount("0.5");
    vaccination.setAmountUnitCode("ML");
    vaccination.setFacilityIdNumber("Facility ID");
    vaccination.setGivenByNumber("Given By");
    vaccination.setIdSubmitter("Submitter Id");
    vaccination.setInformationSourceCode("01");
    vaccination.setLotNumber("Lot Number");
    vaccination.setManufacturerCode("Manufacturer");
  }

  private Date makeDate(int year, int month, int day)
  {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month - 1);
    cal.set(Calendar.DAY_OF_MONTH, day);
    Date date = cal.getTime();
    return date;
  }
}
