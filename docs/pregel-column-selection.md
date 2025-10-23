# Pregel Column Selection Optimization

## Overview

The Pregel column selection optimization is a memory-saving feature that allows you to specify which vertex columns are required when constructing edge triplets, instead of always selecting all vertex columns. This significantly reduces memory usage for algorithms with large vertex state, such as cycle detection and random walks.

## Motivation

In the original Pregel implementation, when constructing triplets, **all** source and destination vertex columns were selected:

```scala
// Before optimization - selects ALL columns
tripletsDF = currentVertices
  .select(struct(col("*")).as(SRC))  // Selects everything!
  .join(edges, ...)
```

This creates a huge dataset in memory, especially for algorithms that have big state (cycle detection, random walks, etc.). In practice, algorithms often only need a **subset** of columns. For example:
- **Rocha-Thatte cycle detection**: Only needs the `sequences` column from source vertices
- **Shortest Paths**: Only needs the `distances` column from both source and destination
- **Label Propagation**: Only needs the `label` column

## API

### Scala API

```scala
def requiredSrcColumns(col: Column, cols: Column*): Pregel
def requiredDstColumns(col: Column, cols: Column*): Pregel
```

### Python API

```python
def requiredSrcColumns(self, col: Column, *cols: Column) -> Pregel
def requiredDstColumns(self, col: Column, *cols: Column) -> Pregel
```

**Important Notes:**
- The `id` column is **always** included automatically (you don't need to specify it)
- The active flag column is **always** included automatically
- If you don't call these methods, all columns are selected (backward compatible)

## Usage Examples

### Example 1: Simple Chain Propagation (Scala)

```scala
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

val vertices = Seq(1, 2, 3, 4, 5).toDF("id")
  .withColumn("extraColumn1", lit("unused"))
  .withColumn("extraColumn2", lit(999))

val edges = Seq((1, 2), (2, 3), (3, 4), (4, 5)).toDF("src", "dst")
val graph = GraphFrame(vertices, edges)

val result = graph.pregel
  .setMaxIter(4)
  .withVertexColumn(
    "value",
    when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
    when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value"))
  )
  .sendMsgToDst(Pregel.src("value"))
  .aggMsgs(max(Pregel.msg))
  .requiredSrcColumns(col("value"))  // Only select "value" from source vertices
  .run()
```

### Example 2: PageRank with Column Selection (Scala)

```scala
val graph = GraphFrame(vertices, edges)
val alpha = 0.15
val numVertices = vertices.count()

val ranks = graph.pregel
  .setMaxIter(10)
  .withVertexColumn(
    "rank",
    lit(1.0 / numVertices),
    coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices
  )
  .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
  .aggMsgs(sum(Pregel.msg))
  .requiredSrcColumns(col("rank"), col("outDegree"))  // Only need rank and outDegree
  .run()
```

### Example 3: Bidirectional Message Passing (Scala)

```scala
val result = graph.pregel
  .setMaxIter(10)
  .withVertexColumn("distance", initExpr, updateExpr)
  .sendMsgToSrc(Pregel.dst("distance") + lit(1))
  .sendMsgToDst(Pregel.src("distance") + lit(1))
  .aggMsgs(min(Pregel.msg))
  .requiredSrcColumns(col("distance"))  // Source needs distance column
  .requiredDstColumns(col("distance"))  // Destination needs distance column
  .run()
```

### Example 4: Multiple Columns (Scala)

```scala
val result = graph.pregel
  .setMaxIter(5)
  .withVertexColumn("sum", initSum, updateSum)
  .withVertexColumn("count", initCount, updateCount)
  .sendMsgToDst(struct(Pregel.src("sum"), Pregel.src("count")))
  .aggMsgs(aggExpr)
  .requiredSrcColumns(col("sum"), col("count"))      // Multiple columns
  .requiredDstColumns(col("sum"), col("count"))
  .run()
```

### Example 5: Python Usage

```python
from pyspark.sql import functions as F
from graphframes import GraphFrame

vertices = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["id"]) \
    .withColumn("extraColumn", F.lit("unused"))
edges = spark.createDataFrame([(1, 2), (2, 3), (3, 4)], ["src", "dst"])
graph = GraphFrame(vertices, edges)

pregel = graph.pregel

result = pregel \
    .setMaxIter(3) \
    .withVertexColumn(
        "value",
        F.when(F.col("id") == 1, 1).otherwise(0),
        F.when(pregel.msg() > F.col("value"), pregel.msg()).otherwise(F.col("value"))
    ) \
    .sendMsgToDst(pregel.src("value")) \
    .aggMsgs(F.max(pregel.msg())) \
    .requiredSrcColumns(F.col("value")) \
    .run()
```

## Real-World Algorithm Examples

### Cycle Detection (Rocha-Thatte)

The Rocha-Thatte cycle detection algorithm maintains sequences of vertex IDs. With column selection, only the `sequences` column needs to be in memory:

```scala
preparedGraph.pregel
  .withVertexColumn("sequences", initSequences, updateSequences)
  .withVertexColumn("foundCycles", foundSequences, updateFound)
  .sendMsgToDst(Pregel.src("sequences"))
  .aggMsgs(aggregateSequences)
  .requiredSrcColumns(col("sequences"))  // Only sequences needed!
  .run()
```

**Before:** Source vertices with sequences + all other vertex columns
**After:** Source vertices with sequences + id + active flag only

### Shortest Paths

Shortest paths only needs the `distances` map:

```scala
preparedGraph.pregel
  .withVertexColumn("distances", initDistances, updateDistances)
  .sendMsgToSrc(incrementDistances(Pregel.dst("distances")))
  .aggMsgs(aggregateDistances)
  .requiredSrcColumns(col("distances"))
  .requiredDstColumns(col("distances"))
  .run()
```

### Label Propagation

For directed graphs, only source labels are needed:

```scala
preparedGraph.pregel
  .withVertexColumn("label", col("id"), keyWithMaxValue(Pregel.msg))
  .sendMsgToDst(Pregel.src("label"))
  .aggMsgs(aggregateLabels)
  .requiredSrcColumns(col("label"))  // Only need labels
  .run()
```

For undirected graphs:

```scala
preparedGraph.pregel
  .withVertexColumn("label", col("id"), keyWithMaxValue(Pregel.msg))
  .sendMsgToDst(Pregel.src("label"))
  .sendMsgToSrc(Pregel.dst("label"))
  .aggMsgs(aggregateLabels)
  .requiredSrcColumns(col("label"))
  .requiredDstColumns(col("label"))
  .run()
```

## Performance Benefits

The column selection optimization provides significant benefits:

1. **Reduced Memory Usage**: Only required columns are loaded into memory during triplet construction
2. **Faster Shuffle Operations**: Less data to shuffle across the cluster
3. **Better Cache Utilization**: Smaller DataFrames fit better in Spark's memory cache
4. **Improved Performance**: Especially noticeable for:
   - Graphs with many vertex attributes
   - Algorithms with large state (arrays, maps, sequences)
   - Algorithms that run many iterations

### Example Memory Savings

Consider a graph with vertices containing:
- `id`: Long (8 bytes)
- `sequences`: Array[Array[Long]] (could be 1KB+ per vertex)
- `foundCycles`: Array[Array[Long]] (could be 1KB+ per vertex)
- `extraAttribute1`: String (100 bytes)
- `extraAttribute2`: Map (500 bytes)

**Without column selection:**
Each triplet loads: id + sequences + foundCycles + extraAttribute1 + extraAttribute2 ≈ 2.7KB per vertex

**With column selection (only sequences):**
Each triplet loads: id + sequences + active_flag ≈ 1.1KB per vertex

**Savings: ~60% memory reduction!**

## Backward Compatibility

The column selection API is **fully backward compatible**:

```scala
// Old code (no column selection) - still works!
graph.pregel
  .withVertexColumn("value", initExpr, updateExpr)
  .sendMsgToDst(msgExpr)
  .aggMsgs(aggExpr)
  .run()  // Selects all columns by default
```

If you don't call `requiredSrcColumns()` or `requiredDstColumns()`, the behavior is identical to before - all columns are selected.

## Best Practices

1. **Always specify required columns** when you know which columns are needed
2. **Don't over-specify** - only include columns that are actually used in message expressions
3. **Consider both directions** - if you send messages to both src and dst, specify both `requiredSrcColumns` and `requiredDstColumns`
4. **Profile your algorithms** - use Spark UI to see the memory impact
5. **Remember edge columns are always available** - you don't need to specify edge columns

## Common Patterns

### Pattern 1: Only Source Columns Needed
```scala
.sendMsgToDst(Pregel.src("someColumn"))
.requiredSrcColumns(col("someColumn"))
```

### Pattern 2: Only Destination Columns Needed
```scala
.sendMsgToSrc(Pregel.dst("someColumn"))
.requiredDstColumns(col("someColumn"))
```

### Pattern 3: Both Source and Destination Columns Needed
```scala
.sendMsgToDst(when(Pregel.dst("col1") =!= Pregel.src("col1"), Pregel.src("col1")))
.requiredSrcColumns(col("col1"))
.requiredDstColumns(col("col1"))
```

### Pattern 4: Multiple Columns
```scala
.sendMsgToDst(struct(Pregel.src("col1"), Pregel.src("col2")))
.requiredSrcColumns(col("col1"), col("col2"))
```

### Pattern 5: With Edge Attributes
```scala
.sendMsgToDst(Pregel.src("col1") + Pregel.edge("weight"))
.requiredSrcColumns(col("col1"))
// Note: No need to specify edge columns
```

## Testing

The feature includes comprehensive tests for:
- ✅ Single column selection (source only)
- ✅ Single column selection (destination only)
- ✅ Both source and destination columns
- ✅ Multiple columns
- ✅ Backward compatibility (no column selection)
- ✅ Edge attributes remain accessible
- ✅ Identical results with and without optimization

See:
- Scala tests: `core/src/test/scala/org/graphframes/lib/PregelSuite.scala`
- Python tests: `python/tests/test_graphframes.py`

## Migration Guide

### Updating Existing Algorithms

1. **Identify which columns are used** in your message expressions
2. **Add `requiredSrcColumns()` and/or `requiredDstColumns()`** calls
3. **Test** to ensure results are identical

Example migration:

```scala
// Before
graph.pregel
  .withVertexColumn("distance", initDist, updateDist)
  .sendMsgToDst(Pregel.src("distance") + lit(1))
  .aggMsgs(min(Pregel.msg))
  .run()

// After (optimized)
graph.pregel
  .withVertexColumn("distance", initDist, updateDist)
  .sendMsgToDst(Pregel.src("distance") + lit(1))
  .aggMsgs(min(Pregel.msg))
  .requiredSrcColumns(col("distance"))  // <-- Add this line
  .run()
```

## Troubleshooting

### Issue: "Column not found" error
**Cause:** You're using a column in your message expression but didn't include it in `requiredSrcColumns` or `requiredDstColumns`

**Solution:** Add the missing column:
```scala
.requiredSrcColumns(col("column1"), col("column2"))
```

### Issue: Results differ from non-optimized version
**Cause:** This should not happen - if it does, it's a bug

**Solution:**
1. Verify you've included all columns used in message expressions
2. Check that edge columns are not being specified (they're always available)
3. File an issue with a minimal reproduction

## Implementation Details

### How It Works

1. User specifies required columns via `requiredSrcColumns()` and `requiredDstColumns()`
2. During triplet construction, instead of `struct(col("*"))`, we build a struct with only:
   - ID column (always included)
   - Active flag column (always included)
   - User-specified columns
3. The triplet join proceeds with the smaller DataFrames
4. All downstream operations work identically

### Code Location

- Scala API: `core/src/main/scala/org/graphframes/lib/Pregel.scala`
- Python API: `python/graphframes/lib/pregel.py`
- Tests:
  - `core/src/test/scala/org/graphframes/lib/PregelSuite.scala`
  - `python/tests/test_graphframes.py`

## References

- [Pregel: A System for Large-Scale Graph Processing](https://doi.org/10.1145/1807167.1807184) (Malewicz et al.)
- [GraphFrames Documentation](https://graphframes.github.io/graphframes/docs/_site/index.html)
