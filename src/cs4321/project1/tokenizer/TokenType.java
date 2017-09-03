package cs4321.project1.tokenizer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nishadmathur on 23/8/17.
 */
public final class TokenType {
    public final Pattern pattern;
    public final Matcher matcher;

    TokenType(String pattern, String expression) {
        this.pattern = Pattern.compile(pattern);
        this.matcher = this.pattern.matcher(expression);
    }
}
