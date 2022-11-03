import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  col,
  concat,
  expr,
  from_json,
  lag,
  lead,
  lit,
  row_number,
  schema_of_json,
  size,
  struct,
  sum,
  when
}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Helper {
  def getSparkSession(sparkMaster: String, appName: String): SparkSession = {
    val conf =
      new SparkConf().setMaster(sparkMaster).set("spark.app.name", appName)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    spark.conf.set("spark.sql.caseSensitive", false)
    spark
  }

  def loadCSV(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .csv(filePath)
  }

  def getJsonSchemaForType(
      spark: SparkSession,
      df: DataFrame,
      jsonColName: String,
      eventElementType: String
  ): String = {
    val jsonSample = df
      .filter(col(jsonColName).contains(eventElementType))
      .select(jsonColName)
      .collect()(0)
      .getString(0)
    val schemaJson = spark
      .range(1)
      .select(schema_of_json(lit(jsonSample)))
      .collect()(0)
      .getString(0)
    schemaJson
  }

  def getJsonSchema(
      spark: SparkSession,
      df: DataFrame,
      jsonColName: String
  ): StructType = {
    import spark.implicits._
    spark.read.json(df.select(col(jsonColName).as[String])).schema
  }

  def flattenSchema(
      schema: StructType,
      parent: Option[String]
  ): Array[String] = {
    // heavily inspired by https://sparkbyexamples.com/spark/spark-flatten-nested-struct-column/
    val columnTag = parent match {
      case Some(parentValue) => s"${parentValue}."
      case None              => ""
    }

    val cols = schema.fields.flatMap(f => {
      f.dataType match {
        case s: StructType =>
          flattenSchema(
            s,
            Some(s"${columnTag}${f.name}")
          ) //s"Struct<${f.name}>"
        case _ => Array(s"${columnTag}${f.name}")
      }
    })

    cols
  }

  def cleanAndFlatten(
      spark: SparkSession,
      resourceCsvPath: String
  ): DataFrame = {
    val m = Map[String, String]()
    val df = Helper.loadCSV(spark, resourceCsvPath)
    val json_schema = Helper.getJsonSchema(spark, df, "match_element")
    println(json_schema.toDDL)
    val df2 = df.withColumn(
      "match_element",
      from_json(col("match_element"), json_schema, m)
    )
    val window = Window.partitionBy("match_id").orderBy("match_element.seqNum")
    val columnsOfInterest = Seq(
      "match_element.seqNum",
      "match_element.eventElementType",
      "match_element.details.scoredBy"
    )
    def lead_lag_columns(
        lead_or_lag: (String, Int) => Column,
        prefix: String
    ) = {
      columnsOfInterest.map(c =>
        lead_or_lag(c, 1).over(window).as(prefix + c.replace(".", "_"))
      )
    }
    // all_cols = Seq("serving_team", "LAG_match_element_seqNum","LAG_.., "*", "LAG_match_element_seqNum")
    val all_cols = Seq(
      col("match_element.server.team").as("serving_team")
    ) ++ lead_lag_columns(lag, "LAG_") ++ Seq(col("*")) ++ lead_lag_columns(
      lead,
      "LEAD_"
    )
    val df3 = df2
      .select(all_cols: _*)
      .filter("match_element.eventElementType = 'PointStarted'")
      // serveid is index number of the serve starting from 1
      .withColumn("serveid", row_number().over(window))
//      .withColumn(
//        "server",
//        when(
//          col("serving_team") === "TeamA",
//          col("match_element.matchStatus.teamAPlayer1")
//        )
//          .otherwise(col("match_element.matchStatus.teamBPlayer1"))
//      )
      .drop("_c0")

//    df3
//      .select(
//        "server",
//        "serving_team",
//        "match_element.matchStatus.teamAPlayer1",
//        "match_element.matchStatus.teamBPlayer1"
//      )
//      .show(200)

    // just the players details
    val dfMatchPlayers = df2
      .filter(
        "match_element.eventElementType = 'MatchStatusUpdate' and match_element.matchStatus.matchState.state = 'PlayersArriveOnCourt'"
      )
      .select(
        "match_id",
        "match_element.matchStatus.teamAPlayer1",
        "match_element.matchStatus.teamBPlayer1"
      )

    val joinCondition = (df3("match_id") === dfMatchPlayers("match_id"))
    val df4 = df3
      .join(dfMatchPlayers, joinCondition, "inner")
      .select(
        df3("*"),
        dfMatchPlayers("teamAPlayer1"),
        dfMatchPlayers("teamBPlayer1")
      )
      .withColumn(
        "server",
        when(col("serving_team") === "TeamB", col("teamBPlayer1"))
          .otherwise(col("teamAPlayer1"))
      )
      .drop(Seq("teamBPlayer1", "teamAPlayer1"): _*)
      .withColumn(
        "LEAD_match_element_eventElementType",
        when(
          col("LEAD_match_element_eventElementType") === "PointScored",
          concat(col("LEAD_match_element_details_scoredBy"), lit(" scored"))
        ).otherwise(col("LEAD_match_element_eventElementType"))
      )
      .withColumn("message_id", col("message_id").cast("int"))

    //    df3.show(5)
    //    dfMatchPlayers.show(5)
    //    df4.printSchema()
    df4.show(35)

    // Now pivot on LAG_match_element_eventElementType
    val prePivotDf = df4
      .groupBy("match_id", "message_id")
      .pivot("LAG_match_element_eventElementType", Seq("PhysioCalled"))
      .agg(lit(1))
      .withColumn("message_id", col("message_id").cast("int"))
      .withColumn("match_id", col("match_id").cast("int"))
      .withColumnRenamed("match_id", "pre_match_id")
      .withColumnRenamed("message_id", "pre_message_id")

    // Now pivot on LEAD_match_element_details_scoredBy
    val leadPivotDf = df4
      .groupBy("match_id", "message_id")
      .pivot(
        "LEAD_match_element_eventElementType",
        Seq("PointFault", "PointLet", "TeamA scored", "TeamB scored")
      )
      .agg(lit(1))
      .withColumn("message_id", col("message_id").cast("int"))
      .withColumn("match_id", col("match_id").cast("int"))
    //.withColumnRenamed("message_id","_mid")

    //    prePivotDf.show()
    //    leadPivotDf.show()

    //    leadPivotDf.printSchema()
    //    prePivotDf.printSchema()

    def joinC1 = prePivotDf.col("pre_message_id") === leadPivotDf.col(
      "message_id"
    ) && (prePivotDf.col("pre_match_id") === leadPivotDf.col("match_id"))
    def joinC(left: DataFrame, right: DataFrame) =
      col("left.message_id") === col("right.message_id") && (col(
        "left.match_id"
      ) === col("right.match_id"))

    val resultDfPartial = prePivotDf
      .join(leadPivotDf, joinC1)

    val resultDf = resultDfPartial
      .join(
        df4,
        resultDfPartial.col("pre_message_id") === df4.col(
          "message_id"
        ) && (resultDfPartial.col("pre_match_id") === df4.col("match_id"))
      )
      .select(
        col("server"),
        col("PhysioCalled"),
        col("serveid"),
        col("PointFault"),
        col("PointLet"),
        col("TeamA scored"),
        col("TeamB scored"),
        df4.col("match_id"),
        df4.col("message_id")
      )
      .na
      .fill(0)

    // bring it ALL together as per requirement, the following is the final result

    resultDf
  }

  def flattenSchema(spark: SparkSession, resourceCsvPath: String): DataFrame = {
    val m = Map[String, String]()
    val df = Helper.loadCSV(spark, resourceCsvPath)
    val json_schema = Helper.getJsonSchema(spark, df, "match_element")
    val df2 = df.withColumn(
      "match_element",
      from_json(col("match_element"), json_schema, m)
    )
    //    df2.printSchema()
    //    root
    //    |-- _c0: string (nullable = true)
    //    |-- match_id: string (nullable = true)
    //    |-- message_id: string (nullable = true)
    //    |-- match_element: struct (nullable = true)
    //    |    |-- delayStatus: string (nullable = true)
    //    |    |-- details: struct (nullable = true)
    //    |    |    |-- pointType: string (nullable = true)
    //    |    |    |-- scoredBy: string (nullable = true)
    //    |    |-- eventElementType: string (nullable = true)
    //    |    |-- faultType: string (nullable = true)
    //    |    |-- matchStatus: struct (nullable = true)
    //    |    |    |-- courtNum: long (nullable = true)
    //    |    |    |-- firstServer: string (nullable = true)
    //    |    |    |-- matchState: struct (nullable = true)
    //    |    |    |    |-- challengeEnded: string (nullable = true)
    //    |    |    |    |-- evaluationStarted: string (nullable = true)
    //    |    |    |    |-- locationTimestamp: string (nullable = true)
    //    |    |    |    |-- playerId: long (nullable = true)
    //    |    |    |    |-- state: string (nullable = true)
    //    |    |    |    |-- team: string (nullable = true)
    //    |    |    |    |-- treatmentEnded: string (nullable = true)
    //    |    |    |    |-- treatmentLocation: string (nullable = true)
    //    |    |    |    |-- treatmentStarted: string (nullable = true)
    //    |    |    |    |-- won: string (nullable = true)
    //    |    |    |-- numSets: long (nullable = true)
    //    |    |    |-- scoringType: string (nullable = true)
    //    |    |    |-- teamAPlayer1: string (nullable = true)
    //    |    |    |-- teamAPlayersDetails: struct (nullable = true)
    //    |    |    |    |-- player1Country: string (nullable = true)
    //    |    |    |    |-- player1Id: string (nullable = true)
    //    |    |    |-- teamBPlayer1: string (nullable = true)
    //    |    |    |-- teamBPlayersDetails: struct (nullable = true)
    //    |    |    |    |-- player1Country: string (nullable = true)
    //    |    |    |    |-- player1Id: string (nullable = true)
    //    |    |    |-- tieBreakType: string (nullable = true)
    //    |    |    |-- tossChooser: string (nullable = true)
    //    |    |    |-- tossWinner: string (nullable = true)
    //    |    |    |-- umpire: string (nullable = true)
    //    |    |    |-- umpireCode: string (nullable = true)
    //    |    |    |-- umpireCountry: string (nullable = true)
    //    |    |-- matchTime: string (nullable = true)
    //    |    |-- nextServer: struct (nullable = true)
    //    |    |    |-- team: string (nullable = true)
    //    |    |-- numOverrules: long (nullable = true)
    //    |    |-- playerId: long (nullable = true)
    //    |    |-- reason: string (nullable = true)
    //    |    |-- score: struct (nullable = true)
    //    |    |    |-- currentGameScore: struct (nullable = true)
    //    |    |    |    |-- gameType: string (nullable = true)
    //    |    |    |    |-- pointsA: string (nullable = true)
    //    |    |    |    |-- pointsB: string (nullable = true)
    //    |    |    |-- currentSetScore: struct (nullable = true)
    //    |    |    |    |-- gamesA: long (nullable = true)
    //    |    |    |    |-- gamesB: long (nullable = true)
    //    |    |    |-- overallSetScore: struct (nullable = true)
    //    |    |    |    |-- setsA: long (nullable = true)
    //    |    |    |    |-- setsB: long (nullable = true)
    //    |    |    |-- previousSetsScore: array (nullable = true)
    //    |    |    |    |-- element: struct (containsNull = true)
    //    |    |    |    |    |-- gamesA: long (nullable = true)
    //    |    |    |    |    |-- gamesB: long (nullable = true)
    //    |    |    |    |    |-- tieBreakScore: struct (nullable = true)
    //    |    |    |    |    |    |-- pointsA: long (nullable = true)
    //    |    |    |    |    |    |-- pointsB: long (nullable = true)
    //    |    |-- seqNum: long (nullable = true)
    //    |    |-- server: struct (nullable = true)
    //    |    |    |-- team: string (nullable = true)
    //    |    |-- team: string (nullable = true)
    //    |    |-- time: string (nullable = true)
    //    |    |-- timestamp: string (nullable = true)
    //    |    |-- won: string (nullable = true)

    val topLevelParent = Option.empty[String]
    val cols = Helper.flattenSchema(df2.schema, topLevelParent)
//    println(cols.mkString(",\n"))

    val columns_to_select = cols.map(c => col(c).as(c.replace(".", "_")))

    // dataframe with flattened schema (except the array!)
    df2.select(columns_to_select: _*)

  }

  def enrichmentAddSecondFlag(
      spark: SparkSession,
      resourceCsvPath: String
  ): DataFrame = {
    val resultDf = Helper.cleanAndFlatten(spark, resourceCsvPath)
    val cols_selectionOrder = Seq(
      "server",
      "PhysioCalled",
      "SecondServe",
      "serveid",
      "PointFault",
      "PointLet",
      "TeamA scored",
      "TeamB scored",
      "match_id"
    )
    val w = Window.partitionBy("match_id").orderBy(col("serveid").asc)
    // If the previous outcome was PointFault then current point is a SecondServe
    resultDf
      .withColumn("SecondServe", lag("PointFault", 1).over(w))
      .select(cols_selectionOrder.head, cols_selectionOrder.tail: _*)
      .na
      .fill(0, Seq("SecondServe"))

  }

  def transformation(
      spark: SparkSession,
      resourceCsvPath: String
  ): DataFrame = {
    val m = Map[String, String]()
    val df = Helper.loadCSV(spark, resourceCsvPath)
    val json_schema = Helper.getJsonSchema(spark, df, "match_element")
    val df2 = df.withColumn(
      "match_element",
      from_json(col("match_element"), json_schema, m)
    )
    val df3 = df2
      .withColumn(
        "A_WINS",
        size(
          expr(
            "filter(match_element.score.previousSetsScore,  x -> x.gamesA > x.gamesB)"
          )
        )
      )
      .withColumn(
        "B_WINS",
        size(
          expr(
            "filter(match_element.score.previousSetsScore,  x -> x.gamesA < x.gamesB)"
          )
        )
      )
      // deal with nulls in previousSetsScore
      .withColumn("A_WINS", when(col("A_WINS") < 0, 0).otherwise(col("A_WINS")))
      .withColumn("B_WINS", when(col("B_WINS") < 0, 0).otherwise(col("B_WINS")))
      // now put the fixed value in a new column
      .withColumn(
        "fixedOverallScore",
        struct(col("A_WINS").as("setsA"), col("B_WINS").as("setsB"))
      )
      .drop(Seq("A_WINS", "B_WINS"): _*)

    df3
  }

  def RND(spark: SparkSession, resourceCsvPath: String): DataFrame = {
    val m = Map[String, String]()
    val df = Helper.loadCSV(spark, resourceCsvPath)
    val json_schema = Helper.getJsonSchema(spark, df, "match_element")
    // convert match_element from 'string' to struct
    val df2 = df
      .withColumn(
        "match_element",
        from_json(col("match_element"), json_schema, m)
      )
      // convert message_id to int for ordering to work properly
      .withColumn("message_id", col("message_id").cast("int"))

    val w = Window
      .partitionBy("match_id")
      .orderBy(col("message_id"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val dfEnrichWithCuSumAces = df2
      .withColumn(
        "IsAce",
        when(col("match_element.details.pointType") === "Ace", lit(1))
          .otherwise(lit(0))
      )
      .withColumn("TotalMatchAces", sum(col("IsAce")).over(w))
      .drop("IsAce")
    //   df3.printSchema()
    //df3.select("match_id", "message_id","TotalMatchAces").show(1000)

    dfEnrichWithCuSumAces

  }

}
