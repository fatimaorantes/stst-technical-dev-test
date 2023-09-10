package org.devtest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object S3toRedshift {
  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("S3toRedshift")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", "**************IMT")
      .config("spark.hadoop.fs.s3a.secret.key", "**********************************Ffdx/")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
      .getOrCreate()

    //S3 folder
    val presentationFolder = "s3a://technical-dev-test/presentation"

    //dim tables
    val dimAdvert = spark.read.parquet(presentationFolder + "/dim-advert")
    val dimApplicant = spark.read.parquet(presentationFolder + "/dim-applicant")
    val dimBenefit = spark.read.parquet(presentationFolder + "/dim-benefit")
    val dimCompany = spark.read.parquet(presentationFolder + "/dim-company")
    val dimDate = spark.read.parquet(presentationFolder + "/dim-date")
    val dimJob = spark.read.parquet(presentationFolder + "/dim-job")
    val dimSkill = spark.read.parquet(presentationFolder + "/dim-skill")

    //bridge tables
    val bridgeApplicantJob = spark.read.parquet(presentationFolder + "/bridge-applicant-job")
    val bridgeBenefitJob = spark.read.parquet(presentationFolder + "/bridge-benefit-job")
    val bridgeSkillApplicant = spark.read.parquet(presentationFolder + "/bridge-skill-applicant")

    //fact tables
    val factActApp = spark.read.parquet(presentationFolder + "/fact-act-app")
    val factActJob = spark.read.parquet(presentationFolder + "/fact-act-job")

    val createTableSQL = """ -- DIMENSIONS
                           |-- Create dim_advert
                           |CREATE TABLE dim_advert(
                           |	jobId VARCHAR(255) NOT NULL,
                           |	advertId VARCHAR(255) NOT NULL,
                           |	activeDays INT,
                           |	applyUrl VARCHAR(MAX),
                           |	publicationDT DATETIME NOT NULL,
                           |	status VARCHAR(50)
                           |);
                           |
                           |-- Create dim_applicant table
                           |CREATE TABLE dim_applicant (
                           |  applicantKey INT NOT NULL,
                           |  firstName VARCHAR(255),
                           |  lastName VARCHAR(255),
                           |  age INT,
                           |  ageGroup VARCHAR(50)
                           |);
                           |
                           |-- Create dim_benefit table
                           |CREATE TABLE dim_benefit (
                           |  benefitValue VARCHAR(255) NOT NULL,
                           |  key INT NOT NULL
                           |);
                           |
                           |-- Create dim_company table
                           |CREATE TABLE dim_company (
                           |  key INT NOT NULL,
                           |  companyName VARCHAR(255)
                           |);
                           |
                           |-- Create dim_date table
                           |CREATE TABLE dim_date (
                           |  date DATE NOT NULL,
                           |  date_key INT NOT NULL,
                           |  year INT NOT NULL,
                           |  month INT NOT NULL,
                           |  day INT NOT NULL,
                           |  day_of_week INT,
                           |  day_of_year INT,
                           |  quarter INT,
                           |  week_of_year INT,
                           |  is_weekend BOOLEAN
                           |);
                           |
                           |-- Create dim_job table
                           |CREATE TABLE dim_job (
                           |  id VARCHAR(255) NOT NULL,
                           |  title VARCHAR(255),
                           |  sector VARCHAR(255),
                           |  city VARCHAR(255),
                           |  companyKey INT NOT NULL
                           |);
                           |
                           |-- Create dim_skill table
                           |CREATE TABLE dim_skill (
                           |  skillValue VARCHAR(255) NOT NULL,
                           |  skillKey INT NOT NULL
                           |);
                           |
                           |-- BRIDGES
                           |-- Create bridge_applicant_job table
                           |CREATE TABLE bridge_applicant_job (
                           |  jobId VARCHAR(255) NOT NULL,
                           |  applicantKey INT NOT NULL,
                           |  applicationDT DATETIME NOT NULL
                           |);
                           |
                           |-- Create bridge_benefit_job table
                           |CREATE TABLE bridge_benefit_job (
                           |  benefitKey INT NOT NULL,
                           |  jobId VARCHAR(255) NOT NULL
                           |);
                           |
                           |-- Create bridge_skill_applicant table
                           |CREATE TABLE bridge_skill_applicant (
                           |  applicantKey INT NOT NULL,
                           |  skillKey INT
                           |);
                           |
                           |--FACTS
                           |-- Create fact_activity_application table
                           |CREATE TABLE fact_activity_application (
                           |  date DATE NOT NULL,
                           |  date_key INT NOT NULL,
                           |  jobId VARCHAR(255),
                           |  ageGroup VARCHAR(50),
                           |  companyKey INT,
                           |  sector VARCHAR(255),
                           |  applications INT
                           |);
                           |
                           |-- Create fact_activity_job table
                           |CREATE TABLE fact_activity_job (
                           |  date DATE NOT NULL,
                           |  date_key INT NOT NULL,
                           |  companyKey INT,
                           |  sector VARCHAR(255),
                           |  jobs_posted INT
                           |);
                           |
                           |-- ADD CONSTRAINTS PK
                           |
                           |-- dim_advert
                           |ALTER TABLE dim_advert ADD PRIMARY KEY (advertId);
                           |
                           |-- dim_applicant
                           |ALTER TABLE dim_applicant ADD PRIMARY KEY (applicantKey);
                           |
                           |-- dim_benefit
                           |ALTER TABLE dim_benefit ADD PRIMARY KEY (key);
                           |
                           |-- dim_company
                           |ALTER TABLE dim_company ADD PRIMARY KEY (key);
                           |
                           |-- dim_date
                           |ALTER TABLE dim_date ADD PRIMARY KEY (date_key);
                           |
                           |-- dim_job
                           |ALTER TABLE dim_job ADD PRIMARY KEY (id);
                           |
                           |-- dim_skill
                           |ALTER TABLE dim_skill ADD PRIMARY KEY (skillKey);
                           |
                           |-- bridge_applicant_job
                           |ALTER TABLE bridge_applicant_job ADD PRIMARY KEY (jobId, applicantKey);
                           |
                           |-- bridge_benefit_job
                           |ALTER TABLE bridge_benefit_job ADD PRIMARY KEY (benefitKey, jobId);
                           |
                           |-- bridge_skill_applicant
                           |ALTER TABLE bridge_skill_applicant ADD PRIMARY KEY (applicantKey, skillKey)
                           |
                           |
                           |-- ADD CONSTRAINTS FK
                           |-- dim_advert
                           |ALTER TABLE dim_advert ADD CONSTRAINT fk_dim_advert_job
                           |FOREIGN KEY (id) REFERENCES dim_job(id);
                           |
                           |-- dim_job
                           |ALTER TABLE dim_job ADD CONSTRAINT fk_dim_job_company
                           |FOREIGN KEY (companyKey) REFERENCES dim_company(key);
                           |
                           |-- bridge_applicant_job
                           |ALTER TABLE bridge_applicant_job ADD CONSTRAINT fk_bridge_applicant_job_applicant
                           |FOREIGN KEY (applicantKey) REFERENCES dim_applicant(applicantKey);
                           |
                           |ALTER TABLE bridge_applicant_job ADD CONSTRAINT fk_bridge_applicant_job_job
                           |FOREIGN KEY (id) REFERENCES dim_job(id);
                           |
                           |-- bridge_benefit_job
                           |ALTER TABLE bridge_benefit_job ADD CONSTRAINT fk_bridge_benefit_job_benefit
                           |FOREIGN KEY (benefitKey) REFERENCES dim_benefit(key);
                           |
                           |ALTER TABLE bridge_benefit_job ADD CONSTRAINT fk_bridge_benefit_job_job
                           |FOREIGN KEY (id) REFERENCES dim_job(id);
                           |
                           |-- bridge_skill_applicant
                           |ALTER TABLE bridge_skill_applicant ADD CONSTRAINT fk_bridge_skill_applicant_skill
                           |FOREIGN KEY (skillKey) REFERENCES dim_skill(skillKey);
                           |
                           |ALTER TABLE bridge_skill_applicant ADD CONSTRAINT fk_bridge_skill_applicant_app
                           |FOREIGN KEY (applicantKey) REFERENCES dim_applicant(applicantKey);
                           |
                           |
                           |-- fact_activity_application
                           |ALTER TABLE fact_activity_application ADD CONSTRAINT fk_fact_activity_application_job
                           |FOREIGN KEY (jobId) REFERENCES dim_job(id);
                           |
                           |ALTER TABLE fact_activity_application ADD CONSTRAINT fk_fact_activity_application_company
                           |FOREIGN KEY (companyKey) REFERENCES dim_company(key);
                           |
                           |ALTER TABLE fact_activity_application ADD CONSTRAINT fk_fact_activity_application_date
                           |FOREIGN KEY (date_key) REFERENCES dim_date(date_key);
                           |
                           |
                           |-- fact_activity_job
                           |ALTER TABLE fact_activity_job ADD CONSTRAINT fk_fact_activity_job_company
                           |FOREIGN KEY (companyKey) REFERENCES dim_company(key);
                           |
                           |ALTER TABLE fact_activity_job ADD CONSTRAINT fk_fact_activity_job_date
                           |FOREIGN KEY (date_key) REFERENCES dim_date(date_key);
                           |
                           | """.stripMargin


    spark.sql(createTableSQL)

    // Create a list of Spark DataFrames and their Redshift table names
    val tables = List(
      (dimDate, "dim_date"),
      (dimCompany, "dim_company"),
      (dimSkill, "dim_skill"),
      (dimBenefit, "dim_benefit"),
      (dimApplicant, "dim_applicant"),
      (dimJob, "dim_job"),
      (dimAdvert, "dim_advert"),
      (bridgeBenefitJob, "bridge_benefit_job"),
      (bridgeSkillApplicant, "bridge_skill_applicant"),
      (bridgeApplicantJob, "bridge_applicant_job"),
      (factActApp, "fact_activity_application"),
      (factActJob, "fact_activity_job")
    )

    // Iterate over the list and write each DataFrame to a Redshift table
    for ((df, tableName) <- tables) {
      df.write

        .format("io.github.spark_redshift_community.spark.redshift")
        .option("url", "jdbc:redshift://<host>:<port>/<database>?user=<user>&password=<password>")
        .option("dbtable", tableName)
        .option("tempdir", "s3://technical-dev-test/temp")
        .option("aws_iam_role", "<role-arn>")
        .option("tempformat", "PARQUET")
        .mode(SaveMode.Overwrite)
        .save()
    }

    // Stop SparkSession
    spark.stop()
  }}
