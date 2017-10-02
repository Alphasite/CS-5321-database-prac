import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.StringReader;

public class ParseUtils {

    public static PlainSelect parseQuery(String query) {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
        PlainSelect select;

        try {
            select = (PlainSelect) parser.Select().getSelectBody();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

        return select;
    }
}
