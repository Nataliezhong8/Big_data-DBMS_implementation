/*
 * @author YinghongZhong(z5233608)
 * @version v1.0
 * It is the assignment to process a movie dataset.
 * In this ass, I will define 2 mappers, 2 reducers and customize 3 Writables
  
 Writables:
 * 1)MoviePairWritable implements WritableComparable<MoviePairWritable>
 *   format: (mID1,mID2)
 * 2)MovieRatingWritable implements Writable
 *   format: mID rating
 * 3)DataWritable implements Writable
 *   format: (uID, rating1, rating2)
 * 4)MovieArray extends ArrayWritable
 *   format: [(uID1, rating1, rating2), (uID2, rating3, rating4)...]
 *   
 * Job1: Movie Pair(get the movie pairs and their ratings from one user)
 * 1)mapper1:extract the data
 *    UserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable>
 *    output key-value pair: uID, (mId rating)
 * 2)reducer1: self-join the column of mID and get the movie pairs as the key
 *    JoinReducer extends Reducer<Text, MovieRatingWritable, MoviePairWritable, DataWritable>
 *    output key-value pair: (mID1,mID2) (uID,rating1,rating2)
 * 
 * Job2:sort and set the output format of data
 * 1)mapper2: extract data
 *    MyMapper extends Mapper<MoviePairWritable, DataWritable, MoviePairWritable, DataWritable>
 *    output key-value pair: (mID1,mID2), (uID,rating1,rating2)
 * 2)reducer2: shuffle and sort the data and reset the value output format as an array
 *    MyReducer extends Reducer<MoviePairWritable, DataWritable, MoviePairWritable, MovieArray>
 *    output key-value pair:(mID1,mID2), [(uID1,rating1,rating2), (uID2,rating1,rating2),...]
 */
//package ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AssigOnez5233608 {
	
	//create a WritableComparable to store the movie pair 
	public static class MoviePairWritable implements WritableComparable<MoviePairWritable> {
		private Text mID1;
		private Text mID2;
		
		public MoviePairWritable() {
			this.mID1 = new Text("");
			this.mID2 = new Text("");
		}
		
		public MoviePairWritable(Text mID1, Text mID2) {
			this.mID1 = mID1;
			this.mID2 = mID2;
		}

		public Text getmID1() {
			return mID1;
		}

		public void setmID1(Text mID1) {
			this.mID1 = mID1;
		}

		public Text getmID2() {
			return mID2;
		}

		public void setmID2(Text mID2) {
			this.mID2 = mID2;
		}

		//override the compareTo(), compare M1 and then compare M2 if thisM1 = thatM1
		@Override
		public int compareTo(MoviePairWritable o) {
			int thisM1 = Integer.parseInt(this.mID1.toString());//change text to string
			int thisM2 = Integer.parseInt(this.mID2.toString());
			int thatM1 = Integer.parseInt(o.getmID1().toString());
			int thatM2 = Integer.parseInt(o.getmID2().toString());
			if (thisM1 < thatM1) {
				return -1;
			} else if (thisM1 > thatM1) {
				return 1;
			} else {
				return (thisM2 < thatM2 ? -1 : (thisM2 == thatM2 ? 0 : 1));
			}
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.mID1.readFields(data);
			this.mID2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.mID1.write(data);
			this.mID2.write(data);
		}

		//format: (mID1,mID2)
		@Override
		public String toString() {
			return "(" + this.mID1.toString() + "," + this.mID2.toString() + ")";
		}
	}
	
	public static class MovieRatingWritable implements Writable {
		private Text mID;
		private IntWritable rating;
		
		public MovieRatingWritable() {
			this.mID = new Text("");
			this.rating = new IntWritable(-1);
		}
		public MovieRatingWritable(Text mID, IntWritable rating) {
			super();
			this.mID = mID;
			this.rating = rating;
		}
		public Text getmID() {
			return mID;
		}
		public void setmID(Text mID) {
			this.mID = mID;
		}
		public IntWritable getRating() {
			return rating;
		}
		public void setRating(IntWritable rating) {
			this.rating = rating;
		}
		
		@Override
		public void readFields(DataInput data) throws IOException {
			this.mID.readFields(data);
			this.rating.readFields(data);
		}
		
		@Override
		public void write(DataOutput data) throws IOException {
			this.mID.write(data);
			this.rating.write(data);
		}
		
		//format: mID rating
		@Override
		public String toString() {
			return this.mID.toString() + " " + this.rating.toString() ;
		}
		
	}
	
	public static class DataWritable implements Writable {
		private Text uID;
		private IntWritable rating1;
		private IntWritable rating2;
		
		public DataWritable() {
			this.uID = new Text("");
			this.rating1 = new IntWritable(-1);
			this.rating2 = new IntWritable(-1);
		}
		public DataWritable(Text uID, IntWritable rating1, IntWritable rating2) {
			super();
			this.uID = uID;
			this.rating1 = rating1;
			this.rating2 = rating2;
		}
		public Text getuID() {
			return uID;
		}
		public void setuID(Text uID) {
			this.uID = uID;
		}
		public IntWritable getRating1() {
			return rating1;
		}
		public void setRating1(IntWritable rating1) {
			this.rating1 = rating1;
		}
		public IntWritable getRating2() {
			return rating2;
		}
		public void setRating2(IntWritable rating2) {
			this.rating2 = rating2;
		}
		
		@Override
		public void readFields(DataInput data) throws IOException {
			this.uID.readFields(data);
			this.rating1.readFields(data);
			this.rating2.readFields(data);
		}
		
		@Override
		public void write(DataOutput data) throws IOException {
			this.uID.write(data);
			this.rating1.write(data);
			this.rating2.write(data);
		}
		
		//format: (uID,rating1,rating2)
		@Override
		public String toString() {
			return "(" + this.uID.toString() + "," + 
		           this.rating1.toString() + "," + this.rating2.toString() + ")";
		}
	}
	
	public static class MovieArray extends ArrayWritable {
		private DataWritable[] moviearray = new DataWritable[0];
		
		public MovieArray() {
			super(DataWritable.class);
		}
		
		public MovieArray(DataWritable[] data) {
			super(DataWritable.class);
			DataWritable[] moviearray = new DataWritable[data.length];
			for (int i = 0; i < data.length; i++) {
				moviearray[i] = new DataWritable(data[i].getuID(), data[i].getRating1(), data[i].getRating2());
			}
			//set the value of the array
			this.setMoviearray(moviearray);
		}
		
		public DataWritable[] getMoviearray() {
			return moviearray;
		}

		public void setMoviearray(DataWritable[] moviearray) {
			this.moviearray = moviearray;
		}
/*
		@Override
		public void readFields(DataInput in) throws IOException {
			moviearray = new DataWritable[in.readInt()]; // construct values ???why readInt
			for (int i = 0; i < moviearray.length; i++) {
				DataWritable value = (DataWritable) WritableFactories.newInstance(DataWritable.class);
				value.readFields(in);
				moviearray[i] = value;
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(moviearray.length);
			for (int i = 0; i < moviearray.length; i++) {
				moviearray[i].write(out);
			}
		}
*/	
		//override the toString function
		@Override
		public String toString() {
			StringBuffer result = new StringBuffer();
			
			result.append("[");
			for(int i = 0; i < this.getMoviearray().length; i++) {
				if(i == this.getMoviearray().length - 1) {
					result.append(this.getMoviearray()[i].toString()).append("]");
				} else {
					result.append(this.getMoviearray()[i].toString()).append(",");
				}
			}
			
			return result.toString();
		}
		
	}
	
	public static class UserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, MovieRatingWritable>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("::"); //value = U1::M1::2::11111111
			MovieRatingWritable val = new MovieRatingWritable(new Text(parts[1]), new IntWritable(Integer.parseInt(parts[2])));
			context.write(new Text(parts[0]), val); //key:uID  value: mID rating
		}	
	}
	
	/*implement the self join on the mID column:
	 1.use 2 ArrayList<String> to make 2 copies of the mID column;
	 2.use 2 loops to traverse the movie pairs
	 */
	public static class JoinReducer extends Reducer<Text, MovieRatingWritable, MoviePairWritable, DataWritable> {

		List<MovieRatingWritable> m = new ArrayList<MovieRatingWritable>();
		
		@Override
		protected void reduce(Text key, Iterable<MovieRatingWritable> values,
				Reducer<Text, MovieRatingWritable, MoviePairWritable, DataWritable>.Context context)
				throws IOException, InterruptedException {
			//inputkey:uID  inputvalue: [(mID1 rating1),(mID2 rating),...]
			//Removes all of the elements from the lists for each reducer
			m.clear(); 
			//copy the movieID to m1 and m2 list
			for(MovieRatingWritable val: values) {
				MovieRatingWritable movie = new MovieRatingWritable(new Text(val.getmID().toString()), new IntWritable(val.getRating().get()));
				m.add(movie);
			}
			
			//get the movie pairs without duplication
			for(MovieRatingWritable e1: m) {
				for(MovieRatingWritable e2: m) {
					int mID1 = Integer.parseInt(e1.getmID().toString());
					int mID2 = Integer.parseInt(e2.getmID().toString());
					
					//to prevent having the same movie pairs which have different order
					if (mID1 < mID2) { 
						MoviePairWritable moviepair = new MoviePairWritable(new Text(e1.getmID()), new Text(e2.getmID()));
						DataWritable data = new DataWritable(new Text(key), new IntWritable(e1.getRating().get()), new IntWritable(e2.getRating().get()));
						context.write(moviepair, data);
					}
				}
			}
		}
		
	}
	
	
	//this mapper is just used to read the outputfile and give the context to reducer to sort,shuffle and reduce
	//inputkey (m1,m2) ; inputvalue (uID,rating1,rating2)
	public static class MyMapper extends Mapper<MoviePairWritable, DataWritable, MoviePairWritable, DataWritable>{

		@Override
		protected void map(MoviePairWritable key, DataWritable value,
				Mapper<MoviePairWritable, DataWritable, MoviePairWritable, DataWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	//reducer2: shuffle and sort the data and reset the value output format as an array
	public static class MyReducer extends Reducer<MoviePairWritable, DataWritable, MoviePairWritable, MovieArray> {
		
		@Override
		protected void reduce(MoviePairWritable key, Iterable<DataWritable> values,
				Reducer<MoviePairWritable, DataWritable, MoviePairWritable, MovieArray>.Context context)
				throws IOException, InterruptedException {
			//set an ArrayList to store all the elements in the values
			List<DataWritable> data = new ArrayList<DataWritable>();
			for(DataWritable val: values) {
				DataWritable v = new DataWritable(new Text(val.getuID()), new IntWritable(val.getRating1().get()), new IntWritable(val.getRating2().get()));
				data.add(v);
			}
			//change the ArrayList to an array
			DataWritable[] a = data.toArray(new DataWritable[data.size()]);
			MovieArray moviearray = new MovieArray(a);
			
			context.write(key, moviearray);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);//set a output path 
		
		Job job1 = Job.getInstance(conf, "Movie Pair");
		job1.setJarByClass(AssigOnez5233608.class);
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(JoinReducer.class);
		job1.setOutputKeyClass(MoviePairWritable.class);
		job1.setOutputValueClass(DataWritable.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieRatingWritable.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
		
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Job job2 = Job.getInstance(conf, "Set List Format");
		job2.setJarByClass(AssigOnez5233608.class);
		job2.setMapperClass(MyMapper.class);
		job2.setReducerClass(MyReducer.class);
		job2.setOutputKeyClass(MoviePairWritable.class);
		job2.setOutputValueClass(MovieArray.class);
		job2.setMapOutputKeyClass(MoviePairWritable.class);
		job2.setMapOutputValueClass(DataWritable.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(out, "out1"));
		FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
	}

}
