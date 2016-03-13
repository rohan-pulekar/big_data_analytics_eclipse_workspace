package a.b;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Assignment_2__Problem_3 {

	private static String[] STOP_WORDS = new String[] { "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
			"has", "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with" };

	public static void main(String[] args) {
		List<String> stopWordsList = Arrays.<String>asList(STOP_WORDS);
		Charset charset = Charset.forName("ISO-8859-1");
		try {
			Path filePath = Paths.get("./files/original_output_of_eulysses", "part-r-00000");
			List<String> lines = Files.readAllLines(filePath, charset);
			List<WordAndItsCount> listOfWordsAndItsCount = new ArrayList<WordAndItsCount>(500080);
			System.out.println("Input file processing commenced...");
			for (String line : lines) {
				String[] stringsOnTheLine = line.split("\t");
				WordAndItsCount wordAndItsCount = new WordAndItsCount();
				wordAndItsCount.setWord(stringsOnTheLine[0]);
				wordAndItsCount.setCount(Integer.parseInt(stringsOnTheLine[1]));
				if (stringsOnTheLine[0] != null) {
					if (stopWordsList.contains(wordAndItsCount.getWord())) {
						continue;
					}
				}
				listOfWordsAndItsCount.add(wordAndItsCount);
			}
			System.out.println("...Input file processing done");

			Collections.sort(listOfWordsAndItsCount);

			System.out.println("Output file processing commenced...");
			FileWriter fileWriter = new FileWriter(new File("./files/parsed_filtered_sorted_top200_op_of_eulysses"));
			List<WordAndItsCount> top200ListOfWordsAndItsCount = listOfWordsAndItsCount.subList(0, 200);
			for (WordAndItsCount wordAndItsCount : top200ListOfWordsAndItsCount) {
				fileWriter.write(String.valueOf(wordAndItsCount));
				fileWriter.write(System.lineSeparator());
			}
			fileWriter.close();
			System.out.println("...Output file processing done");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static class WordAndItsCount implements Comparable<WordAndItsCount> {

		private String word;
		private int count;

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}

		public String toString() {
			return word + "\t" + count;
		}

		@Override
		public int compareTo(WordAndItsCount wordAndItsCount) {
			if (wordAndItsCount != null) {
				return wordAndItsCount.getCount() - this.count;
			}
			return 0;
		}
	}

}
