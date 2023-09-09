package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object S3toRS {
  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("S3toRS")
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
