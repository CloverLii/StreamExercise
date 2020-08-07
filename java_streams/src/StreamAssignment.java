
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

/**
 * A class provides stream assignment implementation template
 */
public class StreamAssignment {

	private static BufferedReader br;
	/**
	 * @param file: a file used to create the word stream
	 * @return a stream of word strings
	 * Implementation Notes:
	 * This method reads a file and generates a word stream. 
	 * In this exercise, a word only contains English letters (i.e. a-z or A-Z), or digits, and
	 * consists of at least two characters. For example, “The”, “tHe”, or "1989" is a word, 
	 * but “89_”, “things,” (containing punctuation) are not. 
	 */ 	
	public static Stream<String> toWordStream(String file) throws IOException{
		
		String regex = "^[a-zA-Z0-9]+$";	// word only contains English letters or digits
		try {
			br = new BufferedReader(new FileReader(file));
			return br.lines() 
					 .flatMap(line -> Arrays.stream(line.split("\\s+")))
					 .map(s -> s.trim())
					 .parallel()
					 .filter(s -> s.length() >= 2)	// word contains at least 2 characters
					 .filter(s -> s.matches(regex));
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * @param file: a file used to create a word stream
	 * @return the number of words in the file
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) counts the number of words in the file
	 *   (3) measures the time of creating the stream and counting  
	 * 
	 */
	public static long wordCount(String file) throws IOException{
		long startBlur = System.currentTimeMillis();
			
		long wordNum = toWordStream(file).count();
		
		long endBlur = System.currentTimeMillis();
		System.out.println("\nCreating and counting the stream took " + (endBlur - startBlur) / 1e3 + " secs.");
		
		return wordNum;
	}
	
	/**
	 * @param file: a file used to create a word stream
	 * @return a list of the unique words, sorted in a reverse alphabetical order.
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) generates a list of unique words, sorted in a reverse alphabetical order
	 * 
	 */
	public static List<String> uniqueWordList(String file)throws IOException{
		List<String> wordList = toWordStream(file)
									.distinct()
									.sorted(Collections.reverseOrder())
									.collect(Collectors.toList());
		//wordList.forEach(s -> System.out.println(s));
		return wordList;
	}
	
	/**
	 * @param file: a file used to create a word stream
	 * @return one of the longest words in the file
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) uses Stream.reduce to find the longest digit number 
	 * 
	 */
	public static Optional<String> longestDigit(String file) throws IOException {
		String regex = "^[0-9]+$";
		try {
			return toWordStream(file) 
					 .filter(s -> s.matches(regex))	//filter digit number
					 .reduce((s1, s2) -> s1.length() > s2.length()? s1: s2);		
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	/**
	 * @param file: a file used to create a word stream
	 * @return the number of words consisting of three letters
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) uses only Stream.reduce (NO other stream operations) 
	 *       to count the number of words containing three letters or digits (case-insensitive).
	 *       i.e. Your code looks like: 
     *       return toWordStream(file).reduce(...); 
	 * @throws IOException 
     *       
	 */
	public static long wordsWithThreeLettersCount(String file) throws IOException{
		return toWordStream(file)
				.reduce(0, (s1, s2) -> {
					if(s2.length() == 3) 
						return s1 + 1;	// s1 = 0
					else
						return s1;
				}, 
					(w1, w2) -> w1 + w2);	// value type conversion	
	}
	
	/**
	 * @param file: a file used to create a word stream
	 * @return the average length of the words (e.g. the average number of letters in a word)
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) uses only Stream.reduce (NO other stream operations) 
	 *       to calculate the total length and total number of words  
     *   (3) the average word length can be calculated separately e.g. return total_length/total_number_of_words 
	 */
	public static double avergeWordlength(String file) throws IOException {
		long totalWordsNum = wordCount(file);		
		long totalWordLen = toWordStream(file)
							.reduce(0, (s1, s2) -> s1 + s2.length(), (w1, w2) -> w1 + w2);	// s1 = 0
		
		System.out.println("\ntotalLenght = " + totalWordLen + ", totalWordNum = " + totalWordsNum);
		return (double)totalWordLen/totalWordsNum;
	}
	
	/**
	 * @param file: a file used to create a word stream 
	 * @return a map contains key-value pairs of a word (i.e. key) and its occurrences (i.e. value)
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) uses Stream.collect, Collectors.groupingBy, etc., to generate a map 
	 *        containing pairs of word and its occurrences.
	 */
	public static Map<String,Integer> toWordCountMap(String file) throws IOException {
		return toWordStream(file)				
      			.collect(
      					Collectors.groupingBy(
      							Function.<String>identity(), 	// key: word
      							// value: occurrence, count words number and convert to int value
      							Collectors.collectingAndThen(Collectors.counting(), Long::intValue))	
      					);
	}
	
	/**
	 * @param file: a file used to create a word stream 
	 * @return a map contains key-value pairs of a number (the length of a word) as key and a set of words with that length as value. 
	 * Implementation Notes:
	 * This method 
	 *   (1) uses the toWordStream method to create a word stream from the given file
	 *   (2) uses Stream.collect, Collectors.groupingBy, etc., to generate a map containing pairs of a number (the length of a word)
	 *    and a set of words with that length
	 * @throws IOException 
	 * 
	 */
	public static Map<Integer,Set<String>> groupWordByLength(String file) throws IOException{
		return toWordStream(file)
				.distinct()
      			.collect(
      					Collectors.groupingBy(
      							s -> s.length(), 	// key: length of words
      							Collectors.toSet())	// value: set of words
      					);
	}
		
	/**
	 * @param BiFunction that takes two parameters (String s1 and String s2) and 
	 *        returns the index of the first occurrence of s2 in s1, or -1 if s2 is not a substring of s1
	 * @param targetFile: a file used to create a line stream
	 * @param targetString:  the string to be searched in the file
	 *  Implementation Notes:
	 *  This method
	 *   (1) uses BufferReader.lines to read in lines of the target file
	 *   (2) uses Stream operation(s) and BiFuction to 
	 *       produce a new Stream that contains a stream of Object[]s with two elements;
	 *       Element 0: the index of the first occurrence of the target string in the line
	 *       Element 1: the text of the line containing the target string
	 *   (3) uses Stream operation(s) to sort the stream of Object[]s in a descending order of the index
	 *   (4) uses Stream operation(s) to print out the first 20 indexes and lines in the following format
	 *           567:<the line text>
	 *           345:<the line text>
	 *           234:<the line text>
	 *           ...  
	 */
	
	static public class MatchLines{
		int index;
		String context;
		MatchLines(int index, String context)
		{
			this.index = index;
			this.context = context;
		}
		public int getIndex(){
			return index;
		}
		public String getContext(){
			return context;
		}
	}
	
	public static void  printLinesFound(BiFunction<String, String, Integer> pf, String targetFile, String targetString) throws IOException{

		try {
			long startBlur = System.currentTimeMillis();
						
			BufferedReader br = new BufferedReader(new FileReader(targetFile));
			LineNumberReader lr = new LineNumberReader(br);
			
			lr.lines()
					.parallel()
					 .filter(line -> (pf.apply(line, targetString) >= 0)? true: false) 	// filter lines containing the targetString
					 .map(line -> {return new MatchLines(lr.getLineNumber(), line);})	// conduct stream of MatchLines(index, content)
					 .sorted(Comparator.comparing(MatchLines::getIndex).reversed())	// reverse according to line index
					  .limit(20)
					  .sequential()
					  .forEach(obj -> System.out.println(obj.index +" :<" + obj.context + ">"));
			
			long endBlur = System.currentTimeMillis();
			System.out.println("\nFunction printLinesFound took " + (endBlur - startBlur) / 1e3 + " secs.");
			lr.close();

		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
		
	 public static void main(String[] args) throws IOException{		 
		 
		 String fileName = "wiki2.xml";
		 
		 long wordsNum = wordCount(fileName);
		 System.out.println("\n1. The number of words is: " + wordsNum);
		 
		 System.out.println("\n3. The size of unique word list is: " + uniqueWordList(fileName).size());
		 
		 System.out.println("\n4. The longest digit is: " + longestDigit(fileName)); 
		 
		 System.out.println("\n5. The number of words with 3 letters is: " + wordsWithThreeLettersCount(fileName));
		 
		 System.out.println("\n6. The average lenght of words is: " + avergeWordlength(fileName));
		 
		 Map<String,Integer> wordCountMap = toWordCountMap(fileName);
		 int theOcurTimes = wordCountMap.get("the");
		 System.out.println("\n7. The number of word -the occurs: " + theOcurTimes);
//		 wordCountMap.forEach((k, v) -> System.out.println(k + " => " + v));
		 
		 Map<Integer,Set<String>> lengthSet = groupWordByLength(fileName);
		 Set<String> fourLenStrings = lengthSet.get(4);
		 System.out.println("\n8. The number of unique words with length 4 is: " + fourLenStrings.size());
//		 for(String s: fourLenStrings)
//			 System.out.print(s + ", ");		 
//		 lengthSet.forEach((k, v) -> System.out.println(k + " => " + v));	
		 
		 BiFunction <String, String, Integer> pf = (curLine, targetWord) -> curLine.indexOf(targetWord);
		 System.out.println("\n9. Print index and line content which containing word-science ");
		 printLinesFound(pf, fileName, "science");
		 
	 }

}
