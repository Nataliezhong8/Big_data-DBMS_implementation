/*
 * @author Yinghong Zhong(z5233608)
 * @version 3.0
 * Date: 2019/11/9 (T3)
 * 
 * Assignment 2: Find the shortest path from the start node to any other nodes
 * and sort the paths based on their path cost
 * 
 * step1: read the input as a graphRDD and cache it which would be used for a lot of times later
 * step2: Construct the initial distance table with all nodes as key and (cost, startnode) as value.
		1)find all nodes using flatMap and then distinct them to find all distinct nodes
		2)for initial table, if key != startnode, cost is infinite, otherwise it should be 0
		3)cache it as initial_dist
		4)copy a same one named pre_dist which would be used in forloop and updated after every loop
 * step3:Forloop by joining graph and  pre_dist
 * 	    1)iteration time: largest time is the count of distance_table
 *      2)in every loop, 
 *       a. pre_dist leftouterjoin graph RDD and update the distance_table
            key: endnode1 
			value:Path(cost from startnode to endnode1, path),
			     (endnode2, cost from endnode1 to endnode2)
 *		 b. create a new_dist by using mapTopair to update the initial_distance RDD
 *          ba. Check if the joinRDD has empty data.
                in leftouterjoin, we would find that some nodes are the last nodes which means 
 *				that these nodes can't go to other nodes. In this case, just copy its data 
 *				from the initial_distanceRDD
 *			bb. Update other endnode2's cost and path if the cost from startnode to endnode1
 *				is not infinite
 *			bc. union the sourceRDD which only include the startnode info which would be used in the next loop and disappear
 *			bd. reduce by key by finding the min path cost 
 *		 c. update pre_dist = new_dist
 *	step4: Sort the RDD by the path length
 *		 1)filter the RDD by filtering by key to find not startnode data because we don't print startnode data in our file
 *		 2)set the path cost as the key and also change the output format of the unreachable nodes
 *         from (Integer.MAX_VALUE, startnode) to (-1, )
 *		 3)sort by key which is the the path cost 
 *		 4)use map function to set the output as a JavaRDD<String>
 * */

//package ass2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AssigTwoz5233608 {

	//create a data type for the output, format: Integer, ArrayList
	public static class Path implements Serializable {
		Integer cost;
		ArrayList path;
		
		public Path(Integer cost, ArrayList path) {
			this.cost = cost;
			this.path = path;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append(cost).append(",");
			
			if (!path.isEmpty()) {
				for(int i = 0; i < path.size(); i++) {
					if (i != path.size()-1) {
						sb.append(path.get(i)).append("-");
					} else {
						sb.append(path.get(i));
					}
				}
			}
			return sb.toString();
		}
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Assignment 2")
				.setMaster("local");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> input = context.textFile(args[1]);
		String startnode = args[0]; //get the start node
		
		//create a startnode RDD which would be helpful in forloop
		List<String> start = Arrays.asList(startnode);
		
		//step1:read the file and cache it which is helpful to reuse this RDD, format: key: startnode, value:(endnode, cost)
		JavaPairRDD<String, Tuple2<String, Integer>> graph = input.mapToPair(new PairFunction<String, String,Tuple2<String, Integer>>(){

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				String[] parts = line.split(",");
				return new Tuple2<String, Tuple2<String, Integer>>(parts[0], new Tuple2<>(parts[1], Integer.parseInt(parts[2])));
			}
		}).cache();
		
		//step2: Construct the initial distance table with all nodes as key and (cost, startnode) as value.
		//find all nodes first
		JavaPairRDD<String, Path> initial_dist = graph.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<String, Integer>>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, Tuple2<String, Integer>> line) throws Exception {
				ArrayList nodes = new ArrayList();
				nodes.add(line._1);
				nodes.add(line._2._1);
				return nodes.iterator();
			}
		}).distinct().mapToPair(new PairFunction<String, String, Path>(){ //construct the initial distance table

			@Override
			public Tuple2<String, Path> call(String node) throws Exception {
				ArrayList pre_path = new ArrayList();
				pre_path.add(startnode);
				if (node.equals(startnode)) { //set cost of startnode to startnode is 0
					return new Tuple2<String, Path>(node, new Path(0, pre_path));
				} else { //set cost of other nodes from startnode is infinite
					return new Tuple2<String, Path>(node, new Path(Integer.MAX_VALUE,pre_path)); 
				}
			}
		}).cache();
		
		JavaPairRDD<String, Path> pre_dist = initial_dist;

		//step3:forloop 
		int count = (int) initial_dist.count();

		for (int i = 0; i < count;i++) { //largest iteration time is initial_dist.count() which is also the number of nodes
			
				JavaPairRDD<String, Tuple2<Path, Optional<Tuple2<String, Integer>>>> joinRDD = pre_dist.leftOuterJoin(graph);				
				
				//create a new distance_table
				JavaPairRDD<String, Path> new_dist = joinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Path, Optional<Tuple2<String, Integer>>>>, String, Path>(){

					@Override
					public Tuple2<String, Path> call(
							Tuple2<String, Tuple2<Path, Optional<Tuple2<String, Integer>>>> line) throws Exception {
						
						ArrayList ori_path = line._2._1.path;
						ArrayList new_path = new ArrayList();
						for (int i = 0; i < ori_path.size(); i++) { //copy the original path first
							new_path.add(ori_path.get(i));
						}
						
						if (line._2._2.isPresent()) {							
							if (line._2._1.cost != Integer.MAX_VALUE) { //if this node is reachable, I update the cost and path
								Integer cost = line._2._1.cost + line._2._2.get()._2;
								new_path.add(line._2._2.get()._1);//add the new node to the path
								Path new_p = new Path(cost, new_path);
								return new Tuple2<String,Path>(line._2._2.get()._1, new_p);
							} else { //it is the unreachable node
								return new Tuple2<String,Path>(line._2._2.get()._1, new Path(Integer.MAX_VALUE, new_path));
							}	
						} else {
							return new Tuple2<String, Path>(line._1, new Path(line._2._1.cost, new_path));
						}
					}
				}).union(initial_dist).reduceByKey(new Function2<Path,Path, Path>(){ //1. union the sourceRDD which only include the startnode info and then reduce by key

					@Override
					public Path call(Path p1, Path p2) throws Exception {
						if (p1.cost < p2.cost) {
							return p1;
						} else {
							return p2;
						}
					}
				});
				pre_dist = new_dist.cache();				
		}
		
		//step4: after get the distance table, I can sort by the path length
		
		JavaRDD<String> result = pre_dist.filter(new Function<Tuple2<String,Path>, Boolean>(){ //filter the data of the startnode which doesn't need to print

			@Override
			public Boolean call(Tuple2<String, Path> node) throws Exception {
				if (node._1.equals(startnode)) {
					return false;
				}else {return true;}
			}
		}).mapToPair(new PairFunction<Tuple2<String,Path>, Integer, Tuple2<String,Path>>(){ //set the path length as key

			@Override
			public Tuple2<Integer, Tuple2<String, Path>> call(Tuple2<String, Path> node) throws Exception {
				int path_cost = node._2.cost;
				
				if (path_cost == Integer.MAX_VALUE) { //this is the unreachable node which only has startnode in the path
					Path no_path = new Path(-1, new ArrayList());
					return new Tuple2<>(Integer.MAX_VALUE, new Tuple2<>(node._1, no_path));
				}
				else {return new Tuple2<>(path_cost, node);}
			}
			
		}).sortByKey().map(new Function<Tuple2<Integer, Tuple2<String, Path>>, String>(){ //sortby path cost and map the value to the stringRDD 

			@Override
			public String call(Tuple2<Integer, Tuple2<String, Path>> node) throws Exception {
				StringBuilder sb = new StringBuilder();
				
				sb.append(node._2._1).append(",").append(node._2._2);
				return sb.toString();
			}
		});

		result.coalesce(1).saveAsTextFile(args[2]);
	}
}
