package cs4321.project1;

import cs4321.project1.tokenizer.Tokenizer;
import cs4321.project1.tokenizer.tokens.Token;
import cs4321.project1.utilities.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nishadmathur on 23/8/17.
 */
public class Lexer {
    public final Tokenizer tokenizer;
    public final String expression;

    public Lexer(String expression) {
        this.tokenizer = new Tokenizer(expression);
        this.expression = expression;
    }

    public List<Token> lex() {
        int startIndex = 0;
        List<Token> tokenStream = new ArrayList<>();

        while (startIndex < expression.length()) {
            Pair<Token, Integer> pair = tokenizer.tokenize(startIndex);
            tokenStream.add(pair.left);
            startIndex = pair.right;
        }

        return tokenStream;
    }
}
