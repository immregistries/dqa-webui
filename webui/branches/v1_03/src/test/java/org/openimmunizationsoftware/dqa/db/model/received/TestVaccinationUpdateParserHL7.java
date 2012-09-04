package org.openimmunizationsoftware.dqa.db.model.received;


import java.text.SimpleDateFormat;
import java.util.List;

import org.openimmunizationsoftware.dqa.db.model.MessageReceived;
import org.openimmunizationsoftware.dqa.db.model.SubmitterProfile;
import org.openimmunizationsoftware.dqa.db.model.received.NextOfKin;
import org.openimmunizationsoftware.dqa.db.model.received.Patient;
import org.openimmunizationsoftware.dqa.db.model.received.Vaccination;
import org.openimmunizationsoftware.dqa.parse.VaccinationUpdateParser;
import org.openimmunizationsoftware.dqa.parse.VaccinationUpdateParserHL7;

import junit.framework.TestCase;


public class TestVaccinationUpdateParserHL7 extends TestCase
{
  
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

  private static final String TEST_1 = "MSH|^~\\&|TCH ImmSys|Nathan's Dev Laptop|TxImmTrac|TxDSHS|20110510095220||VXU^V04|1305039140421.100000004|P|2.4|\r"
    + "PID|||200005643^^^^PI~7833822^^^^MA~453533099^^^^SS||CHEIN^LE^A^JR^DR^^L~ALAST^AFIRST^AMIDDLE^AJR^ADR^A|DOG|20080606|M||2106-3^White^HL70005|102 N BOW WOW TERRACE^APT 103^ROUND ROCK^TX^73377^US^P^^COUNTY||^PRN^^^^512^7999399|||||||||ETHNICITY|BIRTH PLACE|Y|5||||20110604|Y|\r"
    + "PD1|||\r"
    + "NK1|1|CHEIN^LASSIE^THE|MTH^Mother^HL70063|\r"
    + "NK1|2|CHEIN^JACQUES^LE|FTH^Father^HL70063|\r"
    + "PV1||R|\r"
    + "RXA|0|999|20080606|20080606|90707^MMR^CPT^03^MMR^CVX|999|||01^Historical information - source unspecified^NIP001|\r"
    + "RXR|ROUTE|SITE|\r"
    + "RXA|0|999|20080815|20080815|116^^CVX^90680^Rotavirus^C4|999|||01^Historical information - source unspecified^NIP001|\r"
    + "RXA|0|999|20080815|20080815|110^DTaP-Hep B-IPV^CVX^90723^DTaP/Hep B/IPV^C4|999|||01^Historical information - source unspecified^NIP001|\r"
    + "RXA|0|999|20081007|20081007|110^DTaP-Hep B-IPV^CVX^90723^DTaP/Hep B/IPV^C4|999|||00^New immunization record^NIP001||^^^West Houston^^^^^12606 West Houston Center Blvd.^^Houston^TX^77082^US||||D80993X||SKB^GlaxoSmithKline^MVX|\r"
    + "RXA|0|999|20090710|20090710|03^MMR^CVX^90707^MMR^C4|999|||00^New immunization record^NIP001||^^^West Houston^^^^^12606 West Houston Center Blvd.^^Houston^TX^77082^US||||XX993NA||SKB^GlaxoSmithKline^MVX|\r";
  
  public void testCreateVaccinationUpdateMessage()
  {
/*
    SubmitterProfile profile = new SubmitterProfile();
    VaccinationUpdateParser parser = new VaccinationUpdateParserHL7(profile);
    MessageReceived vum = new MessageReceived();
    vum.setRequestText(TEST_1);
    parser.createVaccinationUpdateMessage(vum);
    
    Patient patient = vum.getPatient();
    
    assertEquals("ROUND ROCK", patient.getAddressCity());
    assertEquals("US", patient.getAddressCountryCode());
    assertEquals("COUNTY", patient.getAddressCountyParishCode());
    assertEquals("TX", patient.getAddressStateCode());
    assertEquals("102 N BOW WOW TERRACE", patient.getAddressStreet());
    assertEquals("APT 103", patient.getAddressStreet2());
    assertEquals("P", patient.getAddressTypeCode());
    assertEquals("73377", patient.getAddressZip());
//    assertEquals("AFIRST", patient.getAliasFirst());
//    assertEquals("ALAST", patient.getAliasLast());
//    assertEquals("AMIDDLE", patient.getAliasMiddle());
//    assertEquals("APREFIX", patient.getAliasPrefix());
//    assertEquals("ASUFFIX", patient.getAliasSuffix());
//    assertEquals("A", patient.getAliasTypeCode());
    assertEquals("Y", patient.getBirthMultiple());
    assertEquals("5", patient.getBirthOrderCode());
    assertEquals("BIRTH PLACE", patient.getBirthPlace());
    assertEquals("Y", patient.getDeathIndicator());
    assertNotNull(patient.getDeathDate());
    assertEquals("20110604", sdf.format(patient.getDeathDate()));
    assertEquals("ETHNICITY", patient.getEthnicityCode());
    assertEquals("", patient.getFacilityIdNumber());
    assertEquals("", patient.getFacilityName());
    assertEquals("", patient.getFinancialEligibilityCode());
    assertEquals("7833822", patient.getIdMedicaidNumber());
    assertEquals("453533099", patient.getIdSsnNumber());
    assertEquals("", patient.getIdSubmitterAssigningAuthorityCode());
    assertEquals("200005643", patient.getIdSubmitterNumber());
    assertEquals("PI", patient.getIdSubmitterTypeCode());
    assertEquals("DOG", patient.getMotherMaidenName());
    assertEquals("CHEIN", vum.getPatient().getNameLast());
    assertEquals("LE", vum.getPatient().getNameFirst());
    assertEquals("A", patient.getNameMiddle());
    assertEquals("DR", patient.getNamePrefix());
    assertEquals("JR", patient.getNameSuffix());
    assertEquals("L", patient.getNameTypeCode());
    assertEquals("(512)799-9399", patient.getPhoneNumber());
    assertEquals("", patient.getPhysicianNameLast());
    assertEquals("", patient.getPhysicianNameFirst());
    assertEquals("", patient.getPrimaryLanguageCode());
    assertEquals("", patient.getProtectionCode());
    assertEquals("2106-3", patient.getRaceCode());
    assertEquals("", patient.getRegistryStatusCode());
    assertEquals("M", patient.getSexCode());
    // Next Of Kin
    List<NextOfKin> nextOfKins = vum.getNextOfKins();
    assertEquals(2, nextOfKins.size());
    NextOfKin nextOfKin = nextOfKins.get(0);
    assertEquals("", nextOfKin.getAddressCity());
    assertEquals("", nextOfKin.getAddressCountryCode());
    assertEquals("", nextOfKin.getAddressCountyParishCode());
    assertEquals("", nextOfKin.getAddressStateCode());
    assertEquals("", nextOfKin.getAddressStreet());
    assertEquals("", nextOfKin.getAddressStreet2());
    assertEquals("", nextOfKin.getAddressTypeCode());
    assertEquals("LASSIE", nextOfKin.getNameFirst());
    assertEquals("CHEIN", nextOfKin.getNameLast());
    assertEquals("THE", nextOfKin.getNameMiddle());
    assertEquals("", nextOfKin.getNamePrefix());
    assertEquals("", nextOfKin.getNameSuffix());
    assertEquals("", nextOfKin.getNameTypeCode());
    assertEquals("", nextOfKin.getPhoneNumber());
    assertEquals(1, nextOfKin.getPositionId());
    assertEquals("MTH", nextOfKin.getRelationshipCode());
    // Vaccination
    List<Vaccination> vaccinations = vum.getVaccinations();
    assertEquals(5, vaccinations.size());
    Vaccination vaccination = vaccinations.get(0);
    assertEquals("", vaccination.getActionCode());
    assertEquals("90707", vaccination.getAdminCptCode());
    assertEquals("03", vaccination.getAdminCvxCode());
    assertEquals("20080606", sdf.format(vaccination.getAdminDate()));
    assertEquals("999", vaccination.getAmount());
    assertEquals("", vaccination.getAmountUnitCode());
    assertEquals("", vaccination.getCompletionCode());
    assertEquals("", vaccination.getConfidentialityCode());
    assertEquals("ROUTE", vaccination.getBodyRouteCode());
    assertEquals("SITE", vaccination.getBodySiteCode());
    assertEquals("", vaccination.getEnteredByNameFirst());
    assertEquals("", vaccination.getEnteredByNameLast());
    assertEquals("", vaccination.getEnteredByNumber());
    assertEquals("", vaccination.getFacilityIdNumber());
    assertEquals("", vaccination.getFacilityName());
    assertEquals("", vaccination.getFinancialEligibilityCode());
    assertEquals("", vaccination.getGivenByNameFirst());
    assertEquals("", vaccination.getGivenByNameLast());
    assertEquals("", vaccination.getGivenByNumber());
    assertEquals("", vaccination.getIdSubmitter());
    assertEquals("01", vaccination.getInformationSourceCode());
    assertEquals("", vaccination.getLotNumber());
    assertEquals("", vaccination.getManufacturerCode());
    assertEquals("", vaccination.getOrderedByNameFirst());
    assertEquals("", vaccination.getOrderedByNameLast());
    assertEquals("", vaccination.getOrderedByNumber());
    assertEquals(1, vaccination.getPositionId());
    assertEquals("", vaccination.getRefusalCode());
    assertEquals(null, vaccination.getSystemEntryDate());
    assertEquals(null, vaccination.getVisPublicationDate());
*/
  }

}
