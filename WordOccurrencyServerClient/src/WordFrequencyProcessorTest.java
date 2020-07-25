import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * This class performs testing of WordFrequencyProcessor methods.
 *
 */
public class WordFrequencyProcessorTest {

	/**
	 * The path of test file.
	 */
	private static final String TEST_DATA_FILE_PATH = "trust-test-data.txt";

	/**
	 * @throws java.lang.Exception Thrown by File I/O operations.
	 */
	@Before
	public void setUp() throws Exception {
		FileWriter fw = new FileWriter(new File(TEST_DATA_FILE_PATH));
		String data = "This is first line.\nThis is second line.\nThis is third line.";
		fw.write(data);
		fw.flush();
		fw.close();
		System.out.println("File created: " + TEST_DATA_FILE_PATH);
	}

	/**
	 * @throws java.lang.Exception  Thrown by File I/O operations.
	 */
	@After
	public void tearDown() throws Exception {
		File file = new File(TEST_DATA_FILE_PATH);
		boolean delete = file.delete();
		System.out.println("Test completed. File deleted: " + delete);
	}

	/**
	 * Test method for {@link WordFrequencyProcessor#getFrequency()}.
	 */
	@Test
	public void testGetFrequency() {
		WordFrequencyProcessor processor = new WordFrequencyProcessor();
		try {
			processor.readFile(TEST_DATA_FILE_PATH);
			List<WordFrequency> frequency = processor.getFrequency();
			assertNotNull(frequency);
			assertTrue(6 == frequency.size());

			for (WordFrequency wordFrequency : frequency) {
				String word = wordFrequency.getWord();
				if (word.equals("This")) {
					assertTrue(3 == wordFrequency.getCount());
				}
				if (word.equals("is")) {
					assertTrue(3 == wordFrequency.getCount());
				}
				if (word.equals("line")) {
					assertTrue(3 == wordFrequency.getCount());
				}
				if (word.equals("first")) {
					assertTrue(1 == wordFrequency.getCount());
				}
				if (word.equals("second")) {
					assertTrue(1 == wordFrequency.getCount());
				}
				if (word.equals("third")) {
					assertTrue(1 == wordFrequency.getCount());
				}
			}

		} catch (IOException e) {
			fail("Error reading file");
		}
	}

}
