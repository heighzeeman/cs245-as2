package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, EqualTo, Expression, GreaterThan,
	GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DoubleType}


object Transforms {

	// Check whether a ScalaUDF Expression is our dist UDF
	def isDistUdf(udf: ScalaUDF): Boolean = {
		udf.udfName.getOrElse("") == "dist"
	}

	// Get an Expression representing the dist_sq UDF with the provided
	// arguments
	def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
		ScalaUDF(
			(x1: Double, y1: Double, x2: Double, y2: Double) => {
				val xDiff = x1 - x2
				val yDiff = y1 - y2
				xDiff * xDiff + yDiff * yDiff
			}, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
			udfName = Some("dist_sq"))
	}

	// Return any additional optimization passes here
	def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
		Seq(EliminateZeroDists(spark), DistOnLeft(spark), ReplaceTautologicalComparisons(spark),
			ReplaceDistZeroComparisons(spark), DestroySqRoots(spark), PromoteSortDists(spark))
	}

	case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
				udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
		}
	}

	case class DistOnLeft(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case EqualTo(l: Literal, r : ScalaUDF) if isDistUdf(r) =>
				EqualTo(r, l)
			case GreaterThan(l: Literal, r : ScalaUDF) if isDistUdf(r) =>
				LessThan(r, l)
			case LessThan(l: Literal, r : ScalaUDF) if isDistUdf(r) =>
				GreaterThan(r, l)
			case GreaterThanOrEqual(l: Literal, r : ScalaUDF) if isDistUdf(r) =>
				LessThanOrEqual(r, l)
			case LessThanOrEqual(l: Literal, r : ScalaUDF) if isDistUdf(r) =>
				GreaterThanOrEqual(r, l)
		}
	}

	case class ReplaceTautologicalComparisons(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case GreaterThan(l : ScalaUDF, Literal(d, _)) if isDistUdf(l) && d.asInstanceOf[Double] < 0.0 =>
				Literal(true, BooleanType)
			case LessThan(l: ScalaUDF, Literal(d, _)) if isDistUdf(l) && d.asInstanceOf[Double] <= 0.0 =>
				Literal(false, BooleanType)
			case GreaterThanOrEqual(l : ScalaUDF, Literal(d, _)) if isDistUdf(l) && d.asInstanceOf[Double] <= 0.0 =>
				Literal(true, BooleanType)
			case LessThanOrEqual(l: ScalaUDF, Literal(d, _)) if isDistUdf(l) && d.asInstanceOf[Double] < 0.0 =>
				Literal(false, BooleanType)
			case EqualTo(l: ScalaUDF, Literal(d, _)) if isDistUdf(l) && d.asInstanceOf[Double] < 0.0 =>
				Literal(false, BooleanType)
		}
	}

	case class ReplaceDistZeroComparisons(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case EqualTo(l : ScalaUDF, Literal(d, DoubleType)) if isDistUdf(l) && d.asInstanceOf[Double] == 0.0 =>
				And(EqualTo(l.children(0), l.children(2)), EqualTo(l.children(1), l.children(3)))
		}
	}

	case class DestroySqRoots(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case bc @ BinaryComparison(l : ScalaUDF, r : ScalaUDF) if isDistUdf(l) && isDistUdf(r) =>
				bc.makeCopy(Array(getDistSqUdf(l.children), getDistSqUdf(r.children)))
			case bc @ BinaryComparison(l : ScalaUDF, Literal(d, DoubleType)) if isDistUdf(l) => {
				val d_val = d.asInstanceOf[Double]
				bc.makeCopy(Array(getDistSqUdf(l.children), Literal(d_val * d_val, DoubleType)))
			}
		}
	}

	case class PromoteSortDists(spark: SparkSession) extends Rule[LogicalPlan] {
		def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
			case s @ SortOrder(child : ScalaUDF, _, _, _) if isDistUdf(child) => s.copy(child = getDistSqUdf(child.children))
		}

	}
}
