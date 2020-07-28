package edu.coursera.distributed;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import scala.reflect.internal.Trees.Return;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {
    /**
     * Default constructor.
     */
    private PageRank() {
    }

    /**
     * TODO Given an RDD of websites and their ranks, compute new ranks for all
     * websites and return a new RDD containing the updated ranks.
     *
     * Recall from lectures that given a website B with many other websites linking
     * to it, the updated rank for B is the sum over all source websites of the rank
     * of the source website divided by the number of outbound links from the source
     * website. This new rank is damped by multiplying it by 0.85 and adding that to
     * 0.15. Put more simply:
     *
     * new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to
     * B
     *
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     *
     * The reference solution of sparkPageRank uses the following Spark RDD APIs.
     * However, you are free to develop whatever solution makes the most sense to
     * you which also demonstrates speedup on multiple threads.
     *
     * 1) JavaPairRDD.join 2) JavaRDD.flatMapToPair 3) JavaPairRDD.reduceByKey 4)
     * JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique website
     *              IDs.
     * @param ranks The current ranks of each website, keyed on unique website IDs.
     * @return The new ranks of the websites graph, using the PageRank algorithm to
     *         update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

                 //throw new UnsupportedOperationException();

       JavaPairRDD<Integer, Double> contributions = 
       
                sites
                .join(ranks) // join on the unique id
                .values() // tuple of website and it's rank
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Website, Double>, Integer, Double>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<Integer, Double>> call(Tuple2<Website, Double> t) throws Exception {

        
                        List<Tuple2<Integer, Double>> result = new LinkedList<>();

                        //A->B,C,D,E
                        //Calculate Page rank for B, C, D, E from A 
                        // pr(B) = pr(A)/count(B,C,D,E)
                        //t._1() is A
                        // w = {B,C,D,E}
                        while (t._1().edgeIterator().hasNext()) {

                            //w is the site id for the outgoing links eg. B
                            Integer w = t._1().edgeIterator().next();
                        
                            result.add(new Tuple2<Integer, Double>(w, t._2() / t._1().getNEdges()));


                        }
                        
                        return result;
                    }

                });

                return 
                contributions.
                reduceByKey((Double x, Double y) -> x+y)
                .mapValues(sum -> 0.15 + 0.85*sum);
                
                
                /* JavaPairRDD<Integer,Double> newRanks = contributions
                                                        .reduceByKey((x, y) -> x+y)
                                                        .mapValues(sum -> 0.15 + 0.85*sum); */

                //return contributions;

       

    }

}
