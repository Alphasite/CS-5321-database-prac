import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import org.junit.Test;

import java.io.StringReader;

public class SelectionOperatorTest {

	@Test
	public void testNakedCondition() throws JSQLParserException {
		CCJSqlParser parser = new CCJSqlParser(new StringReader("SELECT * FROM a WHERE 1 < 2 AND 3 = 17"));
	}
}
