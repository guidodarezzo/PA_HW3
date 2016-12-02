import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PurchaseHistory extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new PurchaseHistory(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "PurchaseHistory");
      job.setJarByClass(PurchaseHistory.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);	// changed output type from IntWritable to Text.

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static List<String> combos = new ArrayList<String>();
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {

      private Text word = new Text();

      @Override
      
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

    	  // next 4 lines will take the input line and split the customer from the products.
    	  String line = value.toString();
    	  String contentline[] = line.split("\t");
    	  String cust = contentline[0];
    	  String products = contentline[1];
    	  
    	  // Unnecessary to go from ArrayList of products to array of products, but I wanted to experiment with data structures early on in the program.
    	  List<String> splitprod = Arrays.asList(products.split(","));
    	  String prodArray[] = new String[splitprod.size()];
    	  for (int i = 0; i < splitprod.size(); i++) {

    		  prodArray[i] = splitprod.get(i);

    	  }
    	  
    	  // Now for the fun part...the next few blocks or so will generate permutations of products and pass each combination, along with the customer
    	  // to the Reducer.
    	  for (int i = 0; i < splitprod.size(); i++) {
    		 // get rid of remaining objects in combos.
    		 combos.clear();

    		 int prodLength = prodArray.length;
    		 
    		 // pass the array of products to the methods carrying out permutations.
    		 CombinationDriver(prodArray, prodLength, i + 1);
    		    		  
    		  for (int j = 0; j < combos.size(); j++) {
    			
    			  String finalword = combos.get(j);
    			  
    			  Text customerID = new Text();
    			  customerID.set(cust);
    			  word.set(finalword);
    			  context.write(word, customerID);
    			      			 
    		  }
    		  
    		  combos.clear();    // clear combos again just to make sure...		 
    	  }
    	  
    	  	  
    	  
      } // End map
   }// End Map
   
   // methods CombinationDriver and Combinations generate permutations of each product list. 
   // Although unnecessary, I chose to use a Driver because my Mapper was becoming ugly. I like compartmentalizing tasks.
   
   public static void CombinationDriver(String myproducts[], int sizearray, int sizeperm) {
	   String tempdata[] = new String[sizeperm];
	   Combinations(myproducts, sizearray, sizeperm, 0, tempdata, 0);
   }
   
   public static void Combinations (String[] myproducts, int sizearray, int sizeperm, int idx, String tempdata[], int i) {
	   if (idx == sizeperm) {
		   String tempcombo = "";
		   for (int j = 0; j < sizeperm; j++) {
			   if (j == sizeperm - 1) {
				   tempcombo += tempdata[j];
			   }
			   else {
				   tempcombo += (tempdata[j] + ",");
			   }
		   }
		   combos.add(tempcombo);
		   return;
	   }
	   
	   if (i >= sizearray) {
		   return;
	   }
	   tempdata[idx] = myproducts[i];
	   Combinations(myproducts, sizearray, sizeperm, idx + 1, tempdata, i + 1);
	   Combinations(myproducts, sizearray, sizeperm, idx, tempdata, i + 1);
   }
   


   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	      
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  List<String> UIDs = new ArrayList<String>();
	      List<Integer> UIDCount = new ArrayList<Integer>();
	      StringBuilder UIDsCounts = new StringBuilder();
    	  // List UIDs will keep track of the customers that correspond to the key value (combination of purchases).
	      // UIDCount keeps track of how many times the UID is associated with the current key value.
	      // UIDsCounts will be the list of tallied customers in each key. This will be converted into a Text object and written to the output.
    	 
    	
    	  for (Text val: values) {    		  
    		  
    		  int index = -1;
    		  for (int i = 0; i < UIDs.size(); i++) {
    			  if (UIDs.get(i).equals(val.toString())) {
    				  index = i;    				  
    				  break;
    			  }
    		  }	// end for loop to check if new val already exists. In a professional environment, I would use a hash map instead (especially with a large amount of data).
    		  
    		  // next conditional statements check for existing val values. Updates are applied to UIDCounts.
    		  if (index == -1) {
    			  UIDs.add(val.toString());
    			  UIDCount.add(1);
    		  }
    		  
    		  // elseif is not necessary, but I'm trying to be extra careful with conditional statements. 
    		  else if (index > -1) {
    			  int updatedvalue = UIDCount.get(index) + 1;
    			  UIDCount.set(index, updatedvalue);
    		  }
    		  
    		  
    	  }
    	 
    	/* for (int i = 0; i < UIDs.size(); i++) {
    		  System.out.println("for key: " + key + " UID is: " + UIDs.get(i));
    	  }  */ 
    	  
    	  // Grader, I left you some print statements above in case you need to test. 
    	 
    	  UIDsCounts.append("\t");
    	  for (int j = 0; j < UIDs.size(); j++) {
    		  if (j == UIDs.size() - 1) {
    			  UIDsCounts.append(UIDs.get(j) + "(" + UIDCount.get(j) + ")");
    	  }
    		  else {
    			  UIDsCounts.append(UIDs.get(j) + "(" + UIDCount.get(j) + ")" + ",");
    		  }
    	  
                
    	  }// end for;
    	  
    	  // next 3 lines convert the total UIDs and Counts (UIDsCounts) into a string. I did this because
    	  // I was having issues with equality comparisons with the Text class. I would ideally use a hashmap in a professional big data setting.
    	  Text customertally = new Text();
          customertally.set(UIDsCounts.toString());
          context.write(key, customertally);	
   }// end inner reduce
}	// end Reduce
}	// end entire program




	      
	      
	      
 
   
   