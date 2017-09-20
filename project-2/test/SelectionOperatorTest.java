import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.Test;

public class SelectionOperatorTest {

	@Test
	public void testNakedCondition() throws JSQLParserException {
		CCJSqlParserUtil.parse("SELECT * FROM a WHERE 1 < 2 AND 3 = 17");
	}
}
