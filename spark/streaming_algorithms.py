"""
Streaming Algorithms for F1 Analytics.

Implements advanced big data streaming algorithms:
- HyperLogLog for cardinality estimation
- Bloom Filter for duplicate detection
- Reservoir Sampling for representative data selection
- PageRank for driver influence analysis
- LSH (MinHash) for community detection
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math


def hyperloglog_cardinality(df: DataFrame, column: str, precision: int = 14) -> DataFrame:
    """
    Estimate unique count using HyperLogLog algorithm.
    
    Args:
        df: Input DataFrame
        column: Column to estimate cardinality for
        precision: HyperLogLog precision (4-16), higher = more accurate but more memory
    
    Returns:
        DataFrame with estimated unique count
    """
    m = 2 ** precision
    
    hll_df = df.select(
        column,
        F.hash(F.col(column)).alias("hash_val")
    ).withColumn(
        "register_idx",
        F.expr(f"abs(hash_val) % {m}")
    ).withColumn(
        "leading_zeros",
        F.expr(f"32 - log2(abs(hash_val) / {m} + 1)")
    )
    
    registers = hll_df.groupBy("register_idx").agg(
        F.max("leading_zeros").alias("max_zeros")
    )
    
    alpha_m = 0.7213 / (1 + 1.079 / m) if precision >= 7 else 0.673
    
    estimate = registers.select(
        F.lit(alpha_m * m * m).alias("numerator"),
        F.sum(F.pow(2, -F.col("max_zeros"))).alias("denominator")
    ).select(
        (F.col("numerator") / F.col("denominator")).alias("hll_estimate")
    )
    
    return estimate


def bloom_filter_mark_duplicates(df: DataFrame, key_column: str, fpp: float = 0.01) -> DataFrame:
    """
    Mark potential duplicates using Bloom Filter (approximation via Spark).
    
    Args:
        df: Input DataFrame
        key_column: Column to check for duplicates
        fpp: False positive probability (0.01 = 1%)
    
    Returns:
        DataFrame with 'is_duplicate' boolean column
    """
    if key_column not in df.columns:
        return df.withColumn("is_duplicate", F.lit(False)).withColumn("dedup_bloomfilter_flag", F.lit(True))
    
    if "sequence_number" in df.columns:
        order_col = F.col("sequence_number").asc()
    elif "kafka_timestamp" in df.columns:
        order_col = F.col("kafka_timestamp").asc()
    elif "ingested_at" in df.columns:
        order_col = F.col("ingested_at").asc()
    elif "offset" in df.columns:
        order_col = F.col("offset").asc()
    else:
        return df.withColumn("is_duplicate", F.lit(False)).withColumn("dedup_bloomfilter_flag", F.lit(True))
    
    window = Window.partitionBy(key_column).orderBy(order_col)
    
    return df.withColumn(
        "is_duplicate",
        F.when(F.row_number().over(window) > 1, True).otherwise(False)
    ).withColumn(
        "dedup_bloomfilter_flag",
        ~F.col("is_duplicate")  # True if NOT a duplicate
    )


def reservoir_sample(df: DataFrame, sample_size: int = 1000, seed: int = 42) -> DataFrame:
    """
    Select representative sample using Reservoir Sampling algorithm.
    
    Args:
        df: Input DataFrame
        sample_size: Target sample size
        seed: Random seed for reproducibility
    
    Returns:
        Sampled DataFrame
    """
    return (df
        .withColumn("reservoir_rand", F.rand(seed))
        .withColumn("reservoir_weight", 1.0 / (F.col("reservoir_rand") + 1e-10))
        .orderBy(F.col("reservoir_weight").desc())
        .limit(sample_size)
        .drop("reservoir_rand", "reservoir_weight")
    )


def compute_pagerank(vertices_df: DataFrame, edges_df: DataFrame, 
                     max_iter: int = 10, reset_prob: float = 0.15) -> DataFrame:
    """
    Compute PageRank scores using GraphFrames.
    
    Args:
        vertices_df: Vertices DataFrame with 'id' column
        edges_df: Edges DataFrame with 'src', 'dst', 'weight' columns
        max_iter: Maximum iterations
        reset_prob: Teleport/reset probability (damping factor = 1 - reset_prob)
    
    Returns:
        DataFrame with vertex 'id' and 'pagerank' score
    """
    try:
        from graphframes import GraphFrame
        
        if 'id' not in vertices_df.columns:
            vertices_df = vertices_df.withColumnRenamed(vertices_df.columns[0], 'id')
        
        if 'src' not in edges_df.columns or 'dst' not in edges_df.columns:
            cols = edges_df.columns
            edges_df = edges_df.withColumnRenamed(cols[0], 'src').withColumnRenamed(cols[1], 'dst')
        
        graph = GraphFrame(vertices_df, edges_df)
        
        pagerank_result = graph.pageRank(
            resetProbability=reset_prob,
            maxIter=max_iter
        )
        
        return pagerank_result.vertices.select('id', F.col('pagerank').alias('pagerank_score'))
    
    except ImportError:
        return iterative_pagerank(vertices_df, edges_df, max_iter, reset_prob)


def iterative_pagerank(vertices_df: DataFrame, edges_df: DataFrame,
                       max_iter: int = 10, reset_prob: float = 0.15) -> DataFrame:
    """
    Simple iterative PageRank implementation without GraphFrames.
    
    Args:
        vertices_df: Vertices with 'id' column
        edges_df: Edges with 'src', 'dst' columns
        max_iter: Number of iterations
        reset_prob: Reset probability
    
    Returns:
        DataFrame with 'id' and 'pagerank_score'
    """
    num_vertices = vertices_df.count()
    initial_rank = 1.0 / num_vertices
    
    ranks = vertices_df.select(F.col("id"), F.lit(initial_rank).alias("rank"))
    
    out_degrees = edges_df.groupBy("src").agg(F.count("dst").alias("out_degree"))
    
    for iteration in range(max_iter):
        contribs = (edges_df
            .join(ranks.withColumnRenamed("id", "src"), "src")
            .join(out_degrees, "src")
            .select(
                F.col("dst").alias("id"),
                (F.col("rank") / F.col("out_degree")).alias("contrib")
            )
        )
        
        new_ranks = contribs.groupBy("id").agg(
            F.sum("contrib").alias("sum_contrib")
        ).select(
            F.col("id"),
            (F.lit(reset_prob) + (F.lit(1 - reset_prob) * F.col("sum_contrib"))).alias("rank")
        )
        
        ranks = vertices_df.select("id").join(new_ranks, "id", "left").fillna(reset_prob, subset=["rank"])
    
    return ranks.select(F.col("id"), F.col("rank").alias("pagerank_score"))


def detect_communities_lsh(driver_features_df: DataFrame, 
                           feature_col: str = "features",
                           num_hash_tables: int = 5) -> DataFrame:
    """
    Detect driver communities using MinHash LSH clustering.
    
    Args:
        driver_features_df: DataFrame with driver features (sparse vector column)
        feature_col: Name of feature column (VectorType)
        num_hash_tables: Number of hash tables for LSH
    
    Returns:
        DataFrame with 'driver_id' and 'community_id'
    """
    try:
        from pyspark.ml.feature import MinHashLSH
        
        lsh = MinHashLSH(
            inputCol=feature_col,
            outputCol="hashes",
            numHashTables=num_hash_tables
        )
        
        model = lsh.fit(driver_features_df)
        
        hashed = model.transform(driver_features_df)
        
        community_df = hashed.withColumn(
            "community_id",
            F.concat_ws("_", F.col("hashes")[0])
        )
        
        return community_df.select("driver_id", "community_id")
    
    except ImportError:
        return driver_features_df.withColumn(
            "community_id",
            F.concat(F.lit("comm_"), F.hash(F.col(feature_col)) % 10)
        ).select("driver_id", "community_id")


def compute_cardinality_metrics(df: DataFrame, group_cols: list, unique_col: str) -> DataFrame:
    """
    Compute both exact and HyperLogLog cardinality for comparison.
    
    Args:
        df: Input DataFrame
        group_cols: Columns to group by
        unique_col: Column to count unique values for
    
    Returns:
        DataFrame with exact_count and hll_estimate columns
    """
    exact = df.groupBy(*group_cols).agg(
        F.countDistinct(unique_col).alias("exact_unique_count")
    )
    
    hll = df.groupBy(*group_cols).agg(
        F.approx_count_distinct(unique_col, rsd=0.05).alias("hll_unique_count")
    )
    
    return exact.join(hll, group_cols)
