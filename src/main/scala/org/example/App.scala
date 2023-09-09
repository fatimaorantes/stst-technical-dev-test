package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.log4j.{Level,Logger}

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}

 object S3FileListing {
  def main(args: Array[String]): Unit = {
    // Set the log level to ERROR (or another desired level)
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("S3FileListing")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", "**************IMT")
      .config("spark.hadoop.fs.s3a.secret.key", "**********************************Ffdx/")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
      .getOrCreate()


    val data = spark.read.json("s3a://technical-dev-test/raw/jobs")
    val stgFolder = "s3a://technical-dev-test/stg"
    val presentationFolder = "s3a://technical-dev-test/presentation"
    //data.printSchema()


    //----------------------------------------------------------------------------------
    //  READING FROM RAW/JOBS FOLDER
    //----------------------------------------------------------------------------------

    /* COMPANY */
    val companyDF = data.select("company")
      .distinct()
      .na.drop()
      .orderBy("company")
      .withColumn("key", lit(monotonically_increasing_id().cast(IntegerType) + 1))
      .select(col("key"), col("company").as("companyName"))


    /* BENEFITS */
    val benefits = data.withColumn("benefitValue", explode(col("benefits")))
      .withColumn("jobId", col("id"))
      .select("benefitValue", "jobId")
      .na.drop("all")
      .distinct()

    val benefitsDF = benefits.select("benefitValue").distinct()
      .withColumn("key", lit(monotonically_increasing_id().cast(IntegerType) + 1))

    //BRIDGE
    val benefitsBridge = benefits
      .join(benefitsDF, Seq("benefitValue"), "inner")
      .select(col("key").as("benefitKey"), col("jobId"))
      .drop("benefitValue") //.show() //bridgeJobBenefit


    /* DATES */
    val startDate = java.sql.Date.valueOf("2010-01-01")
    val endDate = java.sql.Date.valueOf("2023-12-31")

    // Generating dates
    val dateRange = Iterator.iterate(startDate)(currentDate => {
      val calendar = java.util.Calendar.getInstance
      calendar.setTime(currentDate)
      calendar.add(java.util.Calendar.DATE, 1)
      new java.sql.Date(calendar.getTimeInMillis)
    }).takeWhile(_.before(endDate)).toList

    val dateRow = dateRange.map(date => Row(date))
    val dateSchema = StructType(Seq(StructField("date", DateType)))

    // Create a DataFrame from the date range
    val dateDF = spark.createDataFrame(spark.sparkContext.parallelize(dateRow), dateSchema) //.show()


    // dimDate
    val calendarDF = dateDF
      .withColumn("date_key", date_format(col("date"), "yyyyMMdd"))
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))
      .withColumn("day_of_week", dayofweek(col("date")))
      .withColumn("day_of_year", dayofyear(col("date")))
      .withColumn("quarter", quarter(col("date")))
      .withColumn("week_of_year", weekofyear(col("date")))
      .withColumn("is_weekend", when(dayofweek(col("date")).isin(1, 7), true).otherwise(false))
    //calendarDF.show()


    /* ADVERTS */
    val advertDF = data.withColumn("jobId", col("id"))
      .withColumn("advertId", col("adverts.id"))
      .withColumn("activeDays", col("adverts.activeDays").cast(IntegerType))
      .withColumn("applyUrl", col("adverts.applyUrl"))
      .withColumn("publicationDT", from_unixtime(col("adverts.publicationDateTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("status", col("adverts.status"))
      .select("jobId", "advertId", "activeDays", "applyUrl", "publicationDT", "status").na.drop("all")


    /* JOBS */
    val job = data
      .withColumn("title", when(col("title").isNull, "Undefined").otherwise(col("title")))
      .withColumn("sector", when(col("sector").isNull, "Undefined").otherwise((col("sector"))))
      .select(col("id"), col("title"), col("city"), col("sector"), col("company").as("companyName"))
      .na.drop("all")

    val jobDF = job.join(companyDF, Seq("companyName"), "inner")
      .select(col("id"), col("title"), col("sector"), col("city"), col("key").as("companyKey"))
      .drop("companyName")


    /* APPLICANT */
    // separate information by applicant and removing arrays completely nulls
    val explodedApplicants = data
      .withColumn("applicant", explode(col("applicants")))
      .filter(col("applicant.firstName").isNotNull &&
        col("applicant.lastName").isNotNull &&
        col("applicant.skills").isNotNull &&
        col("applicant.age").isNotNull &&
        col("applicant.applicationDate").isNotNull)

    // putting together the applicant with the jobId
    val applicantsWithJobId = explodedApplicants.select(
      col("id"),
      col("applicant.firstName").alias("firstName"),
      col("applicant.lastName").alias("lastName"),
      col("applicant.age").alias("age"),
      col("applicant.skills").alias("skills"),
      col("applicant.applicationDate").alias("applicationDate")
    )

    // add applicant_key and cast applicationDate
    val applicantsData = applicantsWithJobId.withColumnRenamed("id", "jobId")
      .withColumn("applicantKey", lit(monotonically_increasing_id().cast(IntegerType) + 1))
      .withColumn("applicationDT", from_unixtime(col("applicationDate"), "yyyy-MM-dd"))


    // adding the age range to applicant's data
    val applicantDF = applicantsData.withColumn("ageGroup", when(col("age") < 18, "under 18")
      .when(col("age") >= 18 && col("age") <= 20, "18-20")
      .when(col("age") >= 21 && col("age") <= 25, "21-25")
      .when(col("age") >= 26 && col("age") <= 30, "26-30")
      .when(col("age") >= 31 && col("age") <= 35, "31-35")
      .when(col("age") >= 36 && col("age") <= 40, "36-40")
      .when(col("age") >= 41 && col("age") <= 45, "41-45")
      .when(col("age") >= 46 && col("age") <= 50, "46-50")
      .when(col("age") >= 51 && col("age") <= 55, "51-55")
      .when(col("age") >= 56 && col("age") <= 60, "56-60")
      .otherwise("up to 60"))
    //applicantDF.show()


    // APPLICANT'S SKILLS - separate skills for each applicant
    val applicantSkill = applicantsData.withColumn("skillValue", explode(col("skills")))
      .select("applicantKey", "skills", "skillValue")


    /* SKILL */
    val skillDF = applicantSkill.select("skillValue").distinct()
      .filter(col("skillValue").isNotNull)
      .withColumn("skillKey", lit(monotonically_increasing_id().cast(IntegerType) + 1))


    /* SKILL-APPLICANT BRIDGE*/
    val skillApplicantBridge = applicantSkill.join(skillDF, Seq("skillValue"), "inner")
      .select(col("applicantKey"), col("skillKey"))
      .drop("skillValue")


    //----------------------------------------------------------------------------------
    //--------------------------------- SEND TO S3 STG ---------------------------------
    //----------------------------------------------------------------------------------
    //applicant
    applicantDF.write.mode("overwrite").parquet(stgFolder + "/applicant")

    //----------------------------------------------------------------------------------
    //--------------------------------- SEND TO S3 PRE ---------------------------------
    //----------------------------------------------------------------------------------
    //dimCompany
    companyDF.write.mode("overwrite").parquet(presentationFolder + "/dim-company")

    //dimBenefit || bridgeBenefitJob
    benefitsDF.write.mode("overwrite").parquet(presentationFolder + "/dim-benefit")
    benefitsBridge.write.mode("overwrite").parquet(presentationFolder + "/bridge-benefit-job")

    //dimDate
    calendarDF.write.mode("overwrite").parquet(presentationFolder + "/dim-date")

    //dimAdvert
    advertDF.write.mode("overwrite").parquet(presentationFolder + "/dim-advert")

    //dimJob
    jobDF.write.mode("overwrite").parquet(presentationFolder + "/dim-job")

    //dimSkill || bridgeSkillApplicant
    skillDF.write.mode("overwrite").parquet(presentationFolder + "/dim-skill")
    skillApplicantBridge.write.mode("overwrite").parquet(presentationFolder + "/bridge-skill-applicant")

    //----------------------------------------------------------------------------------
    //  READING FROM STAGING FOLDER
    //----------------------------------------------------------------------------------

    /* APPLICATION */

    val app = spark.read.parquet(stgFolder + "/applicant")

    //dimApplicant
    val dimApplicant = app.select("applicantKey", "firstName", "lastName", "age", "ageGroup")
      .orderBy("applicantKey")

    //dimApplicant.show()

    /* BRIDGE-APPLICANT-JOB */
    val bridgeAppJob = app.select("jobId", "applicantKey", "applicationDT")
      .orderBy("applicationDT")

    //----------------------------------------------------------------------------------
    //  READING FROM PRESENTATION FOLDER
    //----------------------------------------------------------------------------------

    val dim_date = spark.read.parquet(presentationFolder + "/dim-date")
    val dim_job = spark.read.parquet(presentationFolder + "/dim-job")
    val dim_adv = spark.read.parquet(presentationFolder + "/dim-advert")


    /* FACT ACTIVITY JOB*/

    val jobs_act = dim_job.join(dim_adv, dim_job("id") === dim_adv("jobId"), "inner")
      .select(col("publicationDT").cast(DateType).as("publicationDT"), col("companyKey"), col("sector"), col("jobId"))

    val factActJob = dim_date.join(jobs_act, dim_date("date") === jobs_act("publicationDT"), "left")
      .groupBy("date", "date_key", "companyKey", "sector")
      .agg(count("jobId").as("jobs_posted"))
      .na.fill(0, Seq("jobs_posted"))
      .select("date", "date_key", "companyKey", "sector", "jobs_posted")
      .orderBy("date_key")

    //.show()


    /* FACT ACTIVITY APPLICATION*/

    val job_app = dim_job.join(app, dim_job("id") === app("jobId"), "inner")
      .groupBy("jobId", "ageGroup", "companyKey", "sector", "applicationDT")
      .agg(count("applicantKey").as("applicants"))
      .select("jobId", "ageGroup", "companyKey", "sector", "applicationDT", "applicants")

    //job_app.filter("applicationDT >= '2019-08-09' AND jobId = 'd137f114-b0b8-4cb7-b412-b77a57053097'")


    val factActApp = dim_date.join(job_app, dim_date("date") === job_app("applicationDT"), "left")
      .groupBy("date", "date_key", "jobId", "ageGroup", "companyKey", "sector", "applicationDT", "applicants")
      .agg(sum("applicants").as("applications"))
      .na.fill(0, Seq("applications"))
      .select("date", "date_key", "jobId", "ageGroup", "companyKey", "sector", "applications")
      .orderBy("date_key", "companyKey", "ageGroup")

    //factActApp.filter("date >= '2019-08-09'").show()


    //----------------------------------------------------------------------------------
    //--------------------------------- SEND TO S3 PRE ---------------------------------
    //----------------------------------------------------------------------------------

    //dimApplicant
    dimApplicant.write.mode("overwrite").parquet(presentationFolder + "/dim-applicant")

    //bridgeApplicantJob
    bridgeAppJob.write.mode("overwrite").parquet(presentationFolder + "/bridge-applicant-job")

    //factActJobs
    factActJob.write.mode("overwrite").parquet(presentationFolder + "/fact-act-job")

    //factActApp
    factActApp.write.mode("overwrite").parquet(presentationFolder + "/fact-act-app")

    // Stop SparkSession
    spark.stop()
  }
}