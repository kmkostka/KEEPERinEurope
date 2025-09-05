library(usethis)

usethis::use_git()
usethis::use_package("DatabaseConnector")
usethis::use_package("CohortGenerator")
usethis::use_package("IncidencePrevalence")
usethis::use_package("Keeper")

usethis::use_r("RunStudy")
usethis::use_readme_rmd()
usethis::use_license("Apache-2.0")

usethis::use_git_config(
  user.name  = "kmkostka",
  user.email = "kostka@ohdsi.org"
)


#Instantiate Cohorts
server_dbi  <- "cdm_gold_202501"
user        <- Sys.getenv("user")
password    <- Sys.getenv("password")
port        <- "5432"
host        <- "163.1.65.51"
cdmSchema   <- "public_100k"
writeSchema <- "results"
cohortDatabaseSchema <- "results"
cohortTable <- "longcovid"
server <- "163.1.65.51/cdm_gold_202501"
databaseId <- "CPRDgold"


library(CDMConnector)
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = server_dbi,
                      host     = host,
                      user     = user,
                      password = password,
                      port     = port)

#Creating CDM object
cdm <- CDMConnector::cdmFromCon(
  con,
  cdmSchema = cdmSchema,
  writeSchema = writeSchema)

library(CohortGenerator)

#Passing table names to CohortGenerator to use
cohortTableNames <- CohortGenerator::getCohortTableNames(
  cohortTable          = "longCOVID"
)


#### CUIUMC COHORTS
#Creating Cohorts
createCohortTables(
  connectionDetails    = DatabaseConnector::createConnectionDetails(
    dbms = "postgresql", server = "163.1.65.51/cdm_gold_202501",
    user = user, password = password,
    port = 5432, pathToDriver = "~/jdbcDrivers"),
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames         = cohortTableNames,
)

# install.packages("CirceR")
library(CirceR)

#Loading JSONS
# Point this at the folder where your ATLAS JSON files live
jsonDir <- "inst/cohorts/json"   # or wherever your .json files are
jsonFiles <- list.files(jsonDir, pattern = "\\.json$", full.names = TRUE)

cohortSet <- CohortGenerator::createEmptyCohortDefinitionSet()

for (i in seq_along(jsonFiles)) {
  json <- readChar(jsonFiles[i], file.info(jsonFiles[i])$size)

  # CirceR: JSON -> expression -> OHDSI SQL
  expr <- CirceR::cohortExpressionFromJson(json)
  sql  <- CirceR::buildCohortQuery(expr, options = CirceR::createGenerateOptions(generateStats = TRUE))

  # Cohort id & name (use filename as name; change if you have nicer names)
  cohortId   <- i
  cohortName <- tools::file_path_sans_ext(basename(jsonFiles[i]))

  cohortSet[i, c("cohortId","cohortName","json","sql")] <- list(cohortId, cohortName, json, sql)
  cohortSet$generate[i] <- TRUE
}

# 5) Generate the cohorts
# install.packages("CohortGenerator")
library(CohortGenerator)

connectionDetails <- createConnectionDetails(
  dbms="postgresql", server= server,
  user=Sys.getenv("user"),, password=Sys.getenv("password"),
  port=5432, pathToDriver="~/jdbcDrivers"
)

#when rerunning on a new session:
unlink("results/incremental", recursive = TRUE, force = TRUE)

cohortsGenerated <- generateCohortSet(
  connectionDetails    = connectionDetails,
  cdmDatabaseSchema    = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames     = cohortTableNames,
  cohortDefinitionSet  = cohortSet,
  incremental          = TRUE,
  incrementalFolder    = "results/incremental"
)

library(Keeper)

exportFolder <- file.path(getwd(), "results")
# dm1
keeper_out_dm1 <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 4,
  cohortName = "DM type I",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(201254,435216),
  symptoms = c(0),
  comorbidities = c(201820,442793,443238,4016045,4065354,45757392,4051114, 433968, 375545, 29555009, 4209145, 4034964, 380834, 4299544, 4226354, 4159742, 43530690, 433736,320128,
                    4170226, 40443308, 441267, 4163735, 192963, 85828009),
  drugs = c(741530, 42873378, 45774489,21600713,
            1502809,1502826,1503297,1510202,
            1515249,1516766,1525215,1529331,1530014,1547504,
            1559684,1560171,1580747,1583722,1594973,1597756,19067100,
            1502905,1513876,1516976,1517998,1531601,1544838,1550023,
            1567198,19122121),
  diagnosticProcedures = c(0),
  measurements	= c(3005131,3005446,3005673,3033145,3033819,4018317,4035259,4041697,4144235,4149883,4290342,37021662,37393411,40652732,40653487,40654476,
                   40759806,40785865, 4184637),
  alternativeDiagnosis = c(201820,442793,443238,4016045,4065354,45757392,
                           4051114, 433968, 375545, 29555009, 4209145, 4034964,
                           380834, 4299544, 4226354, 4159742, 43530690, 433736,
                           320128, 4170226, 40443308, 441267, 4163735, 192963,
                           85828009,201826),
  treatmentProcedures = c(40756884, 4143852, 2746768, 2746766,4002199),
  complications =  c(442793)
)

# writing results
# install.packages("readr")
library(readr)

readr::write_csv(keeper_out_dm1, "results/keeper_output_dm1.csv")


# esrd
keeper_out_esrd <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 1,
  cohortName = "ESRD",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(46271022,193782,443611),
  symptoms = c(0),
  comorbidities = c(200528,257628,316139,434610,437233,35623051),
  drugs = c(1344143, 1557272, 936748, 19014878,19038440, 932745, 19035631, 1304643, 1301125, 19045045, 956874, 1154343, 19037038, 950637),
  diagnosticProcedures = c(2211744,4304092,42737578),
  measurements	= c(3028942, 3020149, 40765040, 440529, 4168689, 3000034, 3001802, 3034734, 3004295, 3013682, 3011965, 3018311, 3004239,  3051825,
                   3016647, 3016723, 3017250, 46236952, 3053283, 3049187),
  alternativeDiagnosis = c(197320,761083,4220631,35623051,46271022),
  treatmentProcedures = c(4120120,42737578,45888790),
  complications =  c(80502,133729,140673,432867,434610,436070,439777,42539502)
)

readr::write_csv(keeper_out_esrd, "results/keeper_output_esrd.csv")


#copd
keeper_out_copd <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 2,
  cohortName = "COPD",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(255573),
  symptoms = c(0), # removing as we are looking all time prior
  comorbidities = c(77670,254761,312437,314754,316139,320136,4185932,4268843),
  drugs = c(21605007),
  diagnosticProcedures = c(4040549, 4303062, 40480054, 2106588, 4223086),
  measurements	= c(45875979),
  alternativeDiagnosis = c(317009, 4063381,257907, 260131, 316139),
  treatmentProcedures = c(4141937, 45887822, 4119403),
  complications = c(256449,261325,261880)
)

#debugging concepts
copd_ids <- unique(c(
  255573,                        # doi (disease of interest)
  77670,254761,312437,314754,316139,320136,4185932,4268843,   # comorbidities
  21605007,                      # drugs
  4040549,4303062,40480054,2106588,4223086,                  # procedures
  45875979,                      # measurements
  317009,4063381,257907,260131,316139,                       # alternative dx
  4141937,45887822,4119403,                                  # treatment procs
  256449,261325,261880                                           # complications
))

check_concepts_presence <- function(connectionDetails,
                                    cdmSchema,
                                    conceptIds,
                                    useAncestor = TRUE,
                                    sampleLimitPerDomain = 0L) {
  stopifnot(length(conceptIds) > 0)

  # Build a VALUES list for the concept IDs
  values_sql <- paste0("VALUES ", paste0("(", unique(conceptIds), ")", collapse = ", "))

  sql <- glue::glue("
WITH concepts(concept_id) AS (
  {values_sql}
),
expanded_concepts AS (
  SELECT DISTINCT
    CASE WHEN {if (useAncestor) 1 else 0} = 1 THEN ca.descendant_concept_id ELSE c.concept_id END AS concept_id
  FROM concepts c
  LEFT JOIN {cdmSchema}.concept_ancestor ca
    ON ca.ancestor_concept_id = c.concept_id AND {if (useAncestor) 1 else 0} = 1
),
-- Map each domain table to its concept column
hits AS (
  -- Conditions
  SELECT 'condition_occurrence' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.condition_occurrence co ON co.condition_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = co.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Procedures
  SELECT 'procedure_occurrence' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.procedure_occurrence po ON po.procedure_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = po.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Measurements
  SELECT 'measurement' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.measurement m ON m.measurement_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = m.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Drugs
  SELECT 'drug_exposure' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.drug_exposure de ON de.drug_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = de.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Observations (often useful in primary care)
  SELECT 'observation' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.observation o ON o.observation_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = o.visit_occurrence_id
  GROUP BY ec.concept_id
),
rollup AS (
  SELECT
    concept_id,
    domain,
    n_records,
    (n_records > 0) AS has_data,
    n_inpatient, n_outpatient, n_er
  FROM hits
)
SELECT
  r.concept_id,
  c.concept_name,
  c.domain_id,
  r.domain AS occurrence_table,
  r.n_records,
  r.has_data,
  r.n_inpatient,
  r.n_outpatient,
  r.n_er
FROM rollup r
JOIN {cdmSchema}.concept c ON c.concept_id = r.concept_id
ORDER BY r.n_records DESC;
")

conn <- DatabaseConnector::connect(connectionDetails)
on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

out <- DatabaseConnector::querySql(conn, sql)
# Logical column might come back as integer; coerce nicely
out$HAS_DATA <- out$HAS_DATA == 1 | out$HAS_DATA == TRUE

# Optional: if you want a quick Y/N per *input* concept regardless of descendants:
# Summarise back to the ancestor IDs supplied
out
}

copd_probe <- check_concepts_presence(
  connectionDetails = connectionDetails,
  cdmSchema         = cdmSchema,
  conceptIds        = copd_ids,
  useAncestor       = TRUE
)

drop_these <- subset(copd_probe, !HAS_DATA)$CONCEPT_ID
head(drop_these)

readr::write_csv(keeper_out_copd, "results/keeper_output_copd.csv")


#appendicitis
keeper_out_appendicitis <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 3,
  cohortName = "Appendicitis",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(440448),
  symptoms = c(0),
  comorbidities = c(0),
  drugs = c(45892419, 1836241, 1759842, 1748975, 1746114, 1742253, 1741122,
            1717963, 1717327, 1709170, 1707164, 1702364, 997881),
  diagnosticProcedures = c(724944,724945,724988,725003,725004,725005,725007,725068,725069,2002442,2002685,2002946,2003446,2003447,2003502,2003507,
                           2003510,2003511,2003524,2003783,2006932,2006935,2100901,2100921,2100938,2108883,2108886,2108887,2108888,2108890,2108904,
                           2109089,2109103,2109180,2109181,2109194,2109201,2109310,2109465,2109567,2109586,2109766,2211426,2211427,2211428,2211493,
                           2211514,2211515,2211516,2211639,2211740,2211741,2211742,2211743,2211744,2211768,2211769,2211950,2313699,2313828,2313992,
                           2313993,2722221,2722222,2746590,2746607,2746773,2746810,2747541,2747549,2773682,2773692,2776180,2779576,2793091,2793092,
                           4045438,4052532,4085764,4123999,4167416,4175226,4178367,4207654,4218549,4220239,4230660,4231419,4241100,4249160,4249893,
                           4251314,4253523,42742552,43527935,46257516,46273536),
  measurements	= c(3017732, 3018010, 3013650, 3000905),
  alternativeDiagnosis = c(201618,444089,4171379,43531054),
  treatmentProcedures = c(2000219,2000811,2001423,2002531,2002724,2002747,2002751,2002762,2002785,2002869,2002964,2003510,2003511,
                          2003565,2003626,2003898,2004230,2004269,2004449,2004491,2004503,2004643,2004788,2008268,2100941,2100992,
                          2101056,2101807,2101813,2101877,2101888,2108476,2109017,2109024,2109028,2109040,2109041,2109056,2109063,
                          2109066,2109116,2109146,2109312,2109366,2109432,2109435,2109444,2109453,2109669,2109701,2109748,2110001,
                          2110239,2110257,2110258,2110308,2110316,2110330,2110394,2722201,2722202,2746508,2746510,2747010,2747064,
                          2747277,2750141,2752899,2753378,2753386,2755284,2776677,2776907,2777024,2777438,2779572,2779574,2779577,
                          2779777,2779780,4013040,4018300,4127886,4135441,4148762,4150970,4162987,4179797,4196081,4196678,4216096,
                          4231419,4234536,4242997,4243665,4249749,4265608,4298948,4306298,37312440,40490893,40493226,42739084,46270663,
                          2002909, 2002911, 2002922, 2109139, 2109140, 2109141, 2109142, 2109143, 2109144, 2109145,2722210, 2753382,
                          2753383, 4018156, 4173452, 4198190, 4220986, 4243973),
  complications =  c(444089,4171379,43531054)
)


appendicitis_ids <- unique(c(
  440448,                        # doi (disease of interest)
  45892419, 1836241, 1759842, 1748975, 1746114, 1742253, 1741122,
  1717963, 1717327, 1709170, 1707164, 1702364, 997881, # comorbidities
  45892419, 1836241, 1759842, 1748975, 1746114, 1742253, 1741122,
  1717963, 1717327, 1709170, 1707164, 1702364, 997881,               # drugs
  724944,724945,724988,725003,725004,725005,725007,725068,725069,2002442,2002685,2002946,2003446,2003447,2003502,2003507,
  2003510,2003511,2003524,2003783,2006932,2006935,2100901,2100921,2100938,2108883,2108886,2108887,2108888,2108890,2108904,
  2109089,2109103,2109180,2109181,2109194,2109201,2109310,2109465,2109567,2109586,2109766,2211426,2211427,2211428,2211493,
  2211514,2211515,2211516,2211639,2211740,2211741,2211742,2211743,2211744,2211768,2211769,2211950,2313699,2313828,2313992,
  2313993,2722221,2722222,2746590,2746607,2746773,2746810,2747541,2747549,2773682,2773692,2776180,2779576,2793091,2793092,
  4045438,4052532,4085764,4123999,4167416,4175226,4178367,4207654,4218549,4220239,4230660,4231419,4241100,4249160,4249893,
  4251314,4253523,42742552,43527935,46257516,46273536,                # procedures
  3017732, 3018010, 3013650, 3000905,                     # measurements
  201618,444089,4171379,43531054,                  # alternative dx
  2000219,2000811,2001423,2002531,2002724,2002747,2002751,2002762,2002785,2002869,2002964,2003510,2003511,
  2003565,2003626,2003898,2004230,2004269,2004449,2004491,2004503,2004643,2004788,2008268,2100941,2100992,
  2101056,2101807,2101813,2101877,2101888,2108476,2109017,2109024,2109028,2109040,2109041,2109056,2109063,
  2109066,2109116,2109146,2109312,2109366,2109432,2109435,2109444,2109453,2109669,2109701,2109748,2110001,
  2110239,2110257,2110258,2110308,2110316,2110330,2110394,2722201,2722202,2746508,2746510,2747010,2747064,
  2747277,2750141,2752899,2753378,2753386,2755284,2776677,2776907,2777024,2777438,2779572,2779574,2779577,
  2779777,2779780,4013040,4018300,4127886,4135441,4148762,4150970,4162987,4179797,4196081,4196678,4216096,
  4231419,4234536,4242997,4243665,4249749,4265608,4298948,4306298,37312440,40490893,40493226,42739084,46270663,
  2002909, 2002911, 2002922, 2109139, 2109140, 2109141, 2109142, 2109143, 2109144, 2109145,2722210, 2753382,
  2753383, 4018156, 4173452, 4198190, 4220986, 4243973,                              # treatment procs
  444089,4171379,43531054, 200219                                       # complications
))

check_concepts_presence <- function(connectionDetails,
                                    cdmSchema,
                                    conceptIds,
                                    useAncestor = TRUE,
                                    sampleLimitPerDomain = 0L) {
  stopifnot(length(conceptIds) > 0)

  # Build a VALUES list for the concept IDs
  values_sql <- paste0("VALUES ", paste0("(", unique(conceptIds), ")", collapse = ", "))

  sql <- glue::glue("
WITH concepts(concept_id) AS (
  {values_sql}
),
expanded_concepts AS (
  SELECT DISTINCT
    CASE WHEN {if (useAncestor) 1 else 0} = 1 THEN ca.descendant_concept_id ELSE c.concept_id END AS concept_id
  FROM concepts c
  LEFT JOIN {cdmSchema}.concept_ancestor ca
    ON ca.ancestor_concept_id = c.concept_id AND {if (useAncestor) 1 else 0} = 1
),
-- Map each domain table to its concept column
hits AS (
  -- Conditions
  SELECT 'condition_occurrence' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.condition_occurrence co ON co.condition_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = co.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Procedures
  SELECT 'procedure_occurrence' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.procedure_occurrence po ON po.procedure_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = po.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Measurements
  SELECT 'measurement' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.measurement m ON m.measurement_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = m.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Drugs
  SELECT 'drug_exposure' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.drug_exposure de ON de.drug_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = de.visit_occurrence_id
  GROUP BY ec.concept_id
  UNION ALL
  -- Observations (often useful in primary care)
  SELECT 'observation' AS domain, ec.concept_id,
         COUNT(*) AS n_records,
         SUM(CASE WHEN vo.visit_concept_id = 9201 THEN 1 ELSE 0 END) AS n_inpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9202 THEN 1 ELSE 0 END) AS n_outpatient,
         SUM(CASE WHEN vo.visit_concept_id = 9203 THEN 1 ELSE 0 END) AS n_er
  FROM expanded_concepts ec
  JOIN {cdmSchema}.observation o ON o.observation_concept_id = ec.concept_id
  LEFT JOIN {cdmSchema}.visit_occurrence vo ON vo.visit_occurrence_id = o.visit_occurrence_id
  GROUP BY ec.concept_id
),
rollup AS (
  SELECT
    concept_id,
    domain,
    n_records,
    (n_records > 0) AS has_data,
    n_inpatient, n_outpatient, n_er
  FROM hits
)
SELECT
  r.concept_id,
  c.concept_name,
  c.domain_id,
  r.domain AS occurrence_table,
  r.n_records,
  r.has_data,
  r.n_inpatient,
  r.n_outpatient,
  r.n_er
FROM rollup r
JOIN {cdmSchema}.concept c ON c.concept_id = r.concept_id
ORDER BY r.n_records DESC;
")

conn <- DatabaseConnector::connect(connectionDetails)
on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

out <- DatabaseConnector::querySql(conn, sql)
# Logical column might come back as integer; coerce nicely
out$HAS_DATA <- out$HAS_DATA == 1 | out$HAS_DATA == TRUE

# Optional: if you want a quick Y/N per *input* concept regardless of descendants:
# Summarise back to the ancestor IDs supplied
out
}

appendicitis_probe <- check_concepts_presence(
  connectionDetails = connectionDetails,
  cdmSchema         = cdmSchema,
  conceptIds        = appendicitis_ids,
  useAncestor       = TRUE
)

drop_these <- subset(appendicitis_probe, !HAS_DATA)$CONCEPT_ID
head(drop_these)

readr::write_csv(keeper_out_appendicitis, "results/keeper_output_appendicitis.csv")

##COPD - modified with symptoms

#copd
keeper_out_copdGP <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 2,
  cohortName = "COPD",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(255573),
  symptoms = c(254761,4048218,4087178,4089228,4128691,4128692,254761,4048218,4087178,4089228,4128691,4128692), # removing as we are looking all time prior
  comorbidities = c(77670,254761,312437,314754,316139,320136,4185932,4268843),
  drugs = c(21605007),
  diagnosticProcedures = c(4040549, 4303062, 40480054, 2106588, 4223086),
  measurements	= c(45875979),
  alternativeDiagnosis = c(317009, 4063381,257907, 260131, 316139),
  treatmentProcedures = c(4141937, 45887822, 4119403),
  complications = c(256449,261325,261880)
)

readr::write_csv(keeper_out_copdGP, "results/keeper_output_copdGP.csv")



#### HDS COHORTS
#### HDS COHORTS

library(CohortGenerator)

#Passing table names to CohortGenerator to use
cohortTableNames <- CohortGenerator::getCohortTableNames(
  cohortTable          = "longCOVID_HDS"
)

#Creating Cohorts
createCohortTables(
  connectionDetails    = DatabaseConnector::createConnectionDetails(
    dbms = "postgresql", server = "163.1.65.51/cdm_gold_202501",
    user = user, password = password,
    port = 5432, pathToDriver = "~/jdbcDrivers"),
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames         = cohortTableNames,
)

# install.packages("CirceR")
library(CirceR)

#Loading JSONS
# Point this at the folder where your ATLAS JSON files live
jsonDir <- "inst/cohorts/json"   # or wherever your .json files are
jsonFiles <- list.files(jsonDir, pattern = "\\.json$", full.names = TRUE)
ignore <- c("1191.json", "1192.json","234.json", "499.json")
jsonFiles <- jsonFiles[!basename(jsonFiles) %in% ignore]

cohortSet <- CohortGenerator::createEmptyCohortDefinitionSet()

for (i in seq_along(jsonFiles)) {
  json <- readChar(jsonFiles[i], file.info(jsonFiles[i])$size)

  # CirceR: JSON -> expression -> OHDSI SQL
  expr <- CirceR::cohortExpressionFromJson(json)
  sql  <- CirceR::buildCohortQuery(expr, options = CirceR::createGenerateOptions(generateStats = TRUE))

  # Cohort id & name (use filename as name; change if you have nicer names)
  cohortId   <- i + 4
  cohortName <- tools::file_path_sans_ext(basename(jsonFiles[i]))

  cohortSet[i, c("cohortId","cohortName","json","sql")] <- list(cohortId, cohortName, json, sql)
  cohortSet$generate[i] <- TRUE
}

# 5) Generate the cohorts
# install.packages("CohortGenerator")
library(CohortGenerator)

connectionDetails <- createConnectionDetails(
  dbms="postgresql", server= server,
  user=Sys.getenv("user"),, password=Sys.getenv("password"),
  port=5432, pathToDriver="~/jdbcDrivers"
)

#when rerunning on a new session:
unlink("results/incremental", recursive = TRUE, force = TRUE)

cohortsGenerated <- generateCohortSet(
  connectionDetails    = connectionDetails,
  cdmDatabaseSchema    = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames     = cohortTableNames,
  cohortDefinitionSet  = cohortSet,
  incremental          = TRUE,
  incrementalFolder    = "results/incremental"
)

library(Keeper)

keeper_out_RA <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 8,
  cohortName = "RheumatoidArthritis",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(80809),
  symptoms = c(3397770, 3387368, 3324735, 4112072, 3397636),
  comorbidities = c(3307319, 3384358, 3223614, 3417150),
  drugs = c(1305058, 964339, 1101898, 3194904, 1151789, 1119119, 937368, 40171288, 1314273, 1177480, 1550557),
  diagnosticProcedures = c(3021614, 3046878, 3015183, 3020460, 3422358),
  measurements = c(3021614, 3046878, 3015183, 3020460, 44802283),
  alternativeDiagnosis = c(3363310, 3468123, 3380476, 3400472, 3415846),
  treatmentProcedures = c(3160898, 4152129, 3296240, 3376026),
  complications = c(3256702, 4179799, 3185877, 3186774, 3286162, 3223614, 3281064)
)


## keeper_out_RA <- Keeper::createKeeper(
##  connectionDetails = connectionDetails,
##    databaseId = databaseId,
##    cdmDatabaseSchema = cdmSchema,
##    cohortDatabaseSchema = cohortDatabaseSchema,
##    cohortTable = cohortTable,
##    cohortDefinitionId = 8,
##    cohortName = "RheumatoidArthritis",
##    sampleSize = 1000,
##    assignNewId = FALSE,
##    useAncestor = TRUE,
##    doi = c(80809),
##    symptoms = c(0),
##    comorbidities = c(0),
##    drugs = c(0),
##    diagnosticProcedures = c(0),
##    measurements = c(0),
##    alternativeDiagnosis = c(0),
##    treatmentProcedures = c(0),
##    complications = c(0)
##  )


readr::write_csv(keeper_out_RA, "results/keeper_output_RA.csv")

keeper_out_DMI <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 5,
  cohortName = "DiabetesMellitusTypeI",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(201820),
  symptoms = c(4012368, 192450, 40028866, 4309406, 4223659, 4209145),
  comorbidities = c(138384, 40437585, 4232076, 194992, 434621),
  drugs = c(1502910, 35602717, 1502905, 1596977, 1550023, 1567198, 1544838, 1503297, 1113307),
  diagnosticProcedures = c(3000483, 3004410, 3010084),
  measurements = c(3037110, 3000483, 3026300, 3004410, 3035350, 3024629),
  alternativeDiagnosis = c(201826, 43531006, 193323, 4024659, 4131907, 4145827),
  treatmentProcedures = c(4024603, 40546169),
  complications = c(4174977, 198124, 4301699, 317576, 381316, 317309, 443727, 440383, 439002)
)

readr::write_csv(keeper_out_DMI, "results/keeper_output_DMI.csv")


keeper_out_PE <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 7,
  cohortName = "PulmonaryEmbolism",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(80809),
  symptoms = c(312437, 77670, 254761, 261687, 315078, 135360),
  comorbidities = c(443392, 438485, 4120008, 433736, 4299535, 4125650),
  drugs = c(1367571, 1301025, 1310149, 43013024, 40241331, 45775372, 4061650),
  diagnosticProcedures = c(4175357, 4329513, 40339071, 4224157),
  measurements = c(4108153, 255848, 4329847, 316139, 257581, 132797),
  alternativeDiagnosis = c(233604007, 22298006, 84114007, 13645005, 195967001, 91302008),
  treatmentProcedures = c(34051162, 4021727, 4287324, 4162246, 4239130, 4232891),
  complications = c(8198571, 4317150, 254662, 4120094, 4256228)
)

readr::write_csv(keeper_out_PE, "results/keeper_output_PE.csv")

keeper_out_myocarditis <- Keeper::createKeeper(
  connectionDetails = connectionDetails,
  databaseId = databaseId,
  cdmDatabaseSchema = cdmSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 6,
  cohortName = "Myocarditis",
  sampleSize = 100,
  assignNewId = FALSE,
  useAncestor = TRUE,
  doi = c(80809),
  symptoms = c(77670, 312437, 315078, 4223659, 135360, 44784217),
  comorbidities = c(434621, 40320120, 45892628, 45775965, 43020658),
  drugs = c(1337720, 1321341, 1337860, 1550557, 1177480, 1338512, 800878, 45892628, 45775965),
  diagnosticProcedures = c(40313221, 4230911, 4082987, 4308808, 4065418),
  measurements = c(3033745, 3019800, 3005785, 3031569, 3029435, 3022022),
  alternativeDiagnosis = c(4215140, 4329847, 4138837, 4163710, 4190773, 132797, 4227747, 4118993),
  treatmentProcedures = c(4052536, 4338594, 4137127),
  complications = c(4163710, 43020652, 442310, 4103295, 437894, 313217, 320425, 198571, 4317150)
)

readr::write_csv(keeper_out_myocarditis, "results/keeper_output_myocarditis.csv")
