# stst-technical-dev-test by Fatima Orantes

## a) Data Profiling
The JSON is composed of sections of information directly related to the job, such as: advert information, applicant information, and characteristics and relevant details about the job itself. These sections also contain more detailed information.

The following is the analysis for each section:

**Job Data.**
This data includes descriptive information such as title, location, the company that posted the job (without an identifier, only the company name), the sector within the company where the job is available, and the benefits offered in the hiring process.

**Advert.**
This section includes an alphanumeric identifier, which is constant, because tell us that the job lived in the site. It also contains a URL and information about the advert's status, including active days, publication date, and current status.

**Applicants.**
This list includes descriptive information about every application the job received, such as the date of application, the applicant's name, age, and relevant skills.

## b)	Dataflow

![alt text](https://github.com/fatimaorantes/stst-technical-dev-test/blob/master/img/data-flow.png)

## c)	Data dictionary

dim_advert Table

    jobId: VARCHAR(255) (Primary Key) - Unique identifier for the job.
    advertId: VARCHAR(255) (Primary Key) - Unique identifier for the advertisement.
    activeDays: INT - Number of active days for the advertisement.
    applyUrl: VARCHAR(MAX) - URL for applying to the job.
    publicationDT: DATETIME (Not Null) - Date and time of advertisement publication.
    status: VARCHAR(50) - Status of the advertisement.

dim_applicant Table

    applicantKey: INT (Primary Key) - Unique identifier for the applicant.
    firstName: VARCHAR(255) - First name of the applicant.
    lastName: VARCHAR(255) - Last name of the applicant.
    age: INT - Age of the applicant.
    ageGroup: VARCHAR(50) - Age group of the applicant.

dim_benefit Table

    benefitValue: VARCHAR(255) (Primary Key) - Value of the benefit.
    key: INT (Primary Key) - Unique identifier for the benefit.

dim_company Table

    key: INT (Primary Key) - Unique identifier for the company.
    companyName: VARCHAR(255) - Name of the company.

dim_date Table

    date: DATE (Primary Key) (Not Null) - Date.
    date_key: INT (Primary Key) (Not Null) - Unique identifier for the date.
    year: INT (Not Null) - Year.
    month: INT (Not Null) - Month.
    day: INT (Not Null) - Day.
    day_of_week: INT - Day of the week.
    day_of_year: INT - Day of the year.
    quarter: INT - Quarter of the year.
    week_of_year: INT - Week of the year.
    is_weekend: BOOLEAN - Indicates if it's a weekend.

dim_job Table

    id: VARCHAR(255) (Primary Key) - Unique identifier for the job.
    title: VARCHAR(255) - Title of the job.
    sector: VARCHAR(255) - Sector of the job.
    city: VARCHAR(255) - City where the job is located.
    companyKey: INT (Not Null) - Foreign key referencing the dim_company table.

dim_skill Table

    skillValue: VARCHAR(255) (Primary Key) - Value of the skill.
    skillKey: INT (Primary Key) - Unique identifier for the skill.


bridge_applicant_job Table

    jobId: VARCHAR(255) (Not Null) - Foreign key referencing the dim_job table.
    applicantKey: INT (Not Null) - Foreign key referencing the dim_applicant table.
    applicationDT: DATETIME (Not Null) - Date and time of the application.

bridge_benefit_job Table

    benefitKey: INT (Not Null) - Foreign key referencing the dim_benefit table.
    jobId: VARCHAR(255) (Not Null) - Foreign key referencing the dim_job table.

bridge_skill_applicant Table

    applicantKey: INT (Not Null) - Foreign key referencing the dim_applicant table.
    skillKey: INT - Foreign key referencing the dim_skill table.


fact_activity_application Table

    date: DATE (Not Null) - Date.
    date_key: INT (Not Null) - Foreign key referencing the dim_date table.
    jobId: VARCHAR(255) - Foreign key referencing the dim_job table.
    ageGroup: VARCHAR(50) - Age group.
    companyKey: INT - Foreign key referencing the dim_company table.
    sector: VARCHAR(255) - Sector.
    applications: INT - Number of applications.

fact_activity_job Table

    date: DATE (Not Null) - Date.
    date_key: INT (Not Null) - Foreign key referencing the dim_date table.
    companyKey: INT - Foreign key referencing the dim_company table.
    sector: VARCHAR(255) - Sector.
    jobs_posted: INT - Number of jobs posted.
    
## d)	Dimensional model by fact table
fact_activity_application
![alt text](https://github.com/fatimaorantes/stst-technical-dev-test/blob/master/img/fact_act_app_model.png)

fact_activity_job

![alt text](https://github.com/fatimaorantes/stst-technical-dev-test/blob/master/img/fact_act_job_model.png)

## e) Project structure

- [technical-dev-test] - s3
  - raw
    - jobs
  - stg
    - applicant
  - presentation
    - dim-advert
    - dim-applicant
    - dim-benefit
    - dim-company
    - dim-date
    - dim-job
    - dim-skill
    - bridge-applicant-job
    - bridge-benefit-job
    - bridge-skill-applicant
    - fact-act-app
    - fact-act-job

