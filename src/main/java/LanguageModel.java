import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if(count < threashold) {
				return;
			}
			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1];
			//what is the outputkey?
			//what is the outputvalue?
			if(!(outputKey == null || outputKey.length() < 1)) {
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
			}
			//write key-value to reducer?
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			PriorityQueue<Cell> minHeap = new PriorityQueue<Cell>(n, new Comparator<Cell>(){

				public int compare(Cell a, Cell b) {
					if(a.count < b.count) return -1;
					if(a.count > b.count) return 1;
					return 0;
				}
			});
			for (Text val : values) {
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.valueOf(curValue.split("=")[1].trim());
				if (minHeap.size() < n) {
					minHeap.offer(new Cell(count, word));
				} else {
					if(minHeap.peek().count < count) {
						minHeap.poll();
						minHeap.offer(new Cell(count, word));
					}
				}
			}
			for (int i = 0; i < n; i++) {
				if(minHeap.size() == 0) break;
				Cell temp = minHeap.poll();
				context.write(new DBOutputWritable(key.toString(), temp.words, temp.count), NullWritable.get());
			}
		}
		private class Cell {
			private int count;
			private String words;
			Cell(int count, String words) {
				this.count = count;
				this.words = words;
			}
		}
	}

}

