package Utility

import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}

object UtilFunction {
  def read_schema(args: String): StructType = StructType {
    var sch = new StructType()
    val split_values = args.split(',').toList

    val d_types = Map(
      "int" -> IntegerType, "string" -> StringType, "bool" -> BooleanType
    )
    for (i <- split_values) {
      val colVal = i.trim.split(' ')
      sch = sch.add(colVal(0), d_types(colVal(1)))
    }
    sch
  }
}
/* This is change im adding for branch 1611*/

