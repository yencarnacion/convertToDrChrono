// inspired by 
// http://bayesianconspiracy.blogspot.com/2010/02/executing-arbitrary-sql-on-csv-files.html
package convert

import groovy.sql.Sql
import org.h2.Driver

// Create an h2 jdbc in-memory database,
// calling it what you like, here db1
def db = Sql.newInstance("jdbc:h2:mem:db1","org.h2.Driver")

// Create a table from patients.csv file... 
db.execute("create table patients as select * from csvread('/Users/yamir/Documents/clients/OficinaLopez/201506_Immediata/DataDemografica/tmp/patients.csv')")

// Create a table from insurance.csv file... 
db.execute("create table insurance as select * from csvread('/Users/yamir/Documents/clients/OficinaLopez/201506_Immediata/DataDemografica/tmp/insurance.csv')")

// Create a table from patient_insurance.csv file... 
db.execute("create table patient_insurance as select * from csvread('/Users/yamir/Documents/clients/OficinaLopez/201506_Immediata/DataDemografica/tmp/patient_insurance.csv')")

// Store the sql query in a variable
def sqlquery = """
          SELECT 
          PA.FirstName AS PA_FirstName,
          PA.LastName AS PA_LastName,
          PA.MiddleName AS PA_MiddleName,
          PA.DOB AS PA_DOB,
          PA.SSN AS PA_SSN,
          PA.MR AS PA_MR,
          PA.AdditionalMR AS PA_AdditionalMR,
          PA.GENDER AS PA_GENDER,
          PA.Address1 AS PA_Address1, 
          PA.Address2 AS PA_Address2,
          PA.Address3 AS PA_Address3,
          PA.City AS PA_City,
          PA.State AS PA_State,
          PA.Zip AS PA_Zip,
          PA.MaritalStatus AS PA_MaritalStatus,
          PA.Ethnicity AS PA_Ethnicity,
          PA.Race AS PA_Race,
          PA.Language AS PA_Language,
          PA.HomePhone AS PA_HomePhone,
          PA.WorkPhone AS PA_WorkPhone,
          PA.MobilePhone AS PA_WorkPhone,
          PA.Email AS PA_Email,
          PI.PATIENT_ID AS PI_PATIENT_ID,
          PI.INSURANCE_PLAN_ID AS PI_INSURANCE_PLAN_ID,
          PI.SUBSCRIBER_ID AS PI_SUBSCRIBER_ID,
          PI.GROUP_NUMBER AS PI_GROUP_NUMBER,
          PI.FILLING_STATUS AS PI_FILLING_STATUS,
          PI.EFFECTIVE_DATE AS PI_EFFECTIVE_DATE,
          PI.TERMINATION_DATE AS PI_TERMINATION_DATE,
          PI.RELATIONSHIP AS PI_RELATIONSHIP,
          PI.INSURED_NAME AS PI_INSURED_NAME,
          PI.INSURED_DOB AS PI_INSURED_DOB,
          PI.INSURED_SSN AS PI_INSURED_SSN,
          PI.INSURED_GENDER AS PI_INSURED_GENDER,
          PI.INSURED_Address1 AS PI_INSURED_Address1,
          PI.INSURED_Address2 AS PI_INSURED_Address2,
          PI.INSURED_Address3 AS PI_INSURED_Address3,
          PI.INSURED_City AS PI_INSURED_City,
          PI.State AS PI_State,
          PI.INSURED_Zip AS PI_INSURED_Zip,
          PI.INSURED_PHONE_NO AS PI_INSURED_PHONE_NO,
          IN.INSURANCE_NAME AS IN_INSURANCE_NAME,
          IN.INSURANCE_PLAN_ID AS IN_INSURANCE_PLAN_ID,
          IN.INSURANCE_TYPE AS IN_INSURANCE_TYPE,
          IN.PK_ADDRESS_ID AS IN_PK_ADDRESS_ID,
          IN.Address1 AS IN_Address1,
          IN.Address2 AS IN_Address2,
          IN.Address3 AS IN_Address3,
          IN.City AS IN_City,
          IN.State AS IN_State,
          IN.Zip AS IN_Zip,
          IN.Phone AS IN_Phone,
          IN.Fax AS IN_Fax,
          IN.PayerID AS IN_PayerID
          FROM PATIENTS PA
          INNER JOIN PATIENT_INSURANCE PI ON PA.PatientID = PI.PATIENT_ID 
          INNER JOIN INSURANCE IN ON PI.INSURANCE_PLAN_ID = IN.INSURANCE_PLAN_ID
""";

// Get the column headings
/*
db.eachRow(sqlquery + " limit 1"){row->
  meta = row.getMetaData()
  numCols = meta.getColumnCount()
  headings = (1..numCols).collect{meta.getColumnLabel(it)}
  println headings.join(",")
}
*/
/*
// Execute a normal sql query on this table...
db.eachRow(sqlquery){row->
  meta = row.getMetaData()
  numCols = meta.getColumnCount()
  vals = (0..<numCols).collect{ row[it] ?: "" }
  println vals.join(",")
}
*/

def headings = [
"First",
"Middle",
"Last",
"Suffix",
"Email",
"Cell Phone",
"Home Phone",
"Office Phone",
"Office Phone Extension",
"Street Address",
"City",
"State",
"Zip Code",
"Gender",
"Date of Birth",
"Social Security #",
"Copay",
"Primary Insurance",
"Primary Insurance Plan Name",
"Primary Insurance Id",
"Primary Insurance Group #",
"Primary Insurance Payer Id",
"Secondary Insurance",
"Secondary Insurance Plan Name",
"Secondary Insurance Id",
"Secondary Insurance Group #",
"Secondary Insurance Payer Id",
"Subscriber First",
"Subscriber Middle",
"Subscriber Last",
"Subscriber Suffix",
"Subscriber DOB",
"Subscriber Social Security #",
"Patient Relationship to Subscriber",
"Secondary Subscriber First",
"Secondary Subscriber Middle",
"Secondary Subscriber Last",
"Secondary Subscriber Suffix",
"Secondary Subscriber DOB",
"Secondary Subscriber Social Security #",
"Patient Relationship to Secondary Subscriber",
"Patient Student Status",
"Employer",
"Employer Phone",
"Employer Zip Code",
"Employer State",
"Employer City",
"Language",
"Emergency Contact Name",
"Emergency Contact Phone",
"Signature On File",
"Referring Dr.",
"Referring Doctor NPI #",
"Primary Care Physician",
"Primarcy Care Physician NPI #",
"Date of Last Appointment",
"Date of next follow-up Appointment",
"Follow-up Appointment Reason",
"Notes"
]

println headings.join(",")

db.eachRow(sqlquery + " limit 10"){row->
 listWithVals = []
 listWithVals.add(row["PA_FirstName"] ?: "")
 listWithVals.add(row["PA_MiddleName"] ?: "")
 listWithVals.add(row["PA_LastName"] ?: "")
 println listWithVals.join(",")
}

